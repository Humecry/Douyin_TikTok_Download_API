# ==============================================================================
# Copyright (C) 2021 Evil0ctal
#
# This file is part of the Douyin_TikTok_Download_API project.
#
# This project is licensed under the Apache License 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
#
# Cookie 池管理器 — 多 Cookie 轮转、健康监控、自动获取
# Cookie Pool Manager — Multi-cookie rotation, health monitoring, auto-acquisition
#
# ==============================================================================

import asyncio
import json
import os
import time
import hashlib
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional, List, Dict

from crawlers.utils.logger import logger

# ==============================================================================
# 数据模型
# ==============================================================================


@dataclass
class CookieEntry:
    """单个 Cookie 条目及其元数据"""

    cookie: str                                    # Cookie 字符串
    source: str = "manual"                         # 来源: manual / browser / webhook / config
    added_at: float = field(default_factory=time.time)
    last_used_at: float = 0.0
    success_count: int = 0
    fail_count: int = 0
    consecutive_fails: int = 0
    weight: float = 1.0                            # 轮转权重 (越大越优先)
    status: str = "healthy"                        # healthy / suspect / dead

    @property
    def fingerprint(self) -> str:
        """Cookie 的短指纹，用于日志和去重"""
        return hashlib.md5(self.cookie.encode()).hexdigest()[:8]

    @property
    def is_usable(self) -> bool:
        return self.status != "dead"


# ==============================================================================
# Cookie 池
# ==============================================================================


class CookiePool:
    """
    多 Cookie 池管理器。

    特性:
    - 从 config.yaml 读取 Cookie 作为种子（向后兼容）
    - 从 cookie_store.json 加载/持久化
    - 加权轮转：成功率高的 Cookie 更优先
    - 自动降级：连续失败 N 次 → suspect → dead
    - 支持 browser_cookie3 自动提取
    - 支持 Chrome 扩展 Webhook 入池
    """

    # 类级别单例缓存
    _instances: Dict[str, "CookiePool"] = {}

    # Cookie 存储目录（相对于 crawlers/utils/）
    STORE_DIR = Path(__file__).resolve().parent

    # 最大池容量
    MAX_POOL_SIZE = 10

    # 连续失败阈值
    SUSPECT_THRESHOLD = 2     # 连续失败 2 次 → suspect
    DEAD_THRESHOLD = 5        # 连续失败 5 次 → dead

    # 请求最小间隔（秒），防止同 Cookie 高频请求触发频控
    MIN_REQUEST_INTERVAL = 3.0

    # 健康检查间隔（秒）
    HEALTH_CHECK_INTERVAL = 300  # 5 分钟

    def __init__(self, service: str):
        """
        Args:
            service: 服务名称，如 "douyin" 或 "tiktok"
        """
        self.service = service
        self._cookies: List[CookieEntry] = []
        self._index: int = 0
        self._lock = asyncio.Lock()
        self._last_health_check: float = 0.0
        self._store_path = self.STORE_DIR / f"cookie_store_{service}.json"
        self._config_path: Optional[Path] = None      # config.yaml 路径
        self._config_cookie_key: Optional[str] = None  # 在 config 中的键路径

        # 加载持久化数据
        self._load()

    # ------------------------------------------------------------------
    # 工厂方法
    # ------------------------------------------------------------------

    @classmethod
    def get_instance(cls, service: str) -> "CookiePool":
        """获取或创建服务对应的单例池"""
        if service not in cls._instances:
            cls._instances[service] = cls(service)
        return cls._instances[service]

    # ------------------------------------------------------------------
    # Cookie 获取
    # ------------------------------------------------------------------

    async def get_cookie(self) -> Optional[str]:
        """
        获取一个可用的 Cookie（加权轮转）。

        Returns:
            Cookie 字符串；若池为空则返回 None
        """
        async with self._lock:
            usable = [c for c in self._cookies if c.is_usable]
            if not usable:
                logger.warning(f"[CookiePool:{self.service}] 池中无可用 Cookie")
                return None

            # 按权重加权随机选择（轮转兜底）
            # 先尝试轮转到下一个，如果那个 Cookie 的 last_used_at 太近则跳过
            now = time.time()
            start_idx = self._index % len(usable)

            for offset in range(len(usable)):
                idx = (start_idx + offset) % len(usable)
                entry = usable[idx]

                # 检查请求间隔
                if now - entry.last_used_at < self.MIN_REQUEST_INTERVAL:
                    continue

                entry.last_used_at = now
                self._index = (idx + 1) % len(usable)
                self._save()
                return entry.cookie

            # 所有 Cookie 都在冷却中，返回最近最少使用的那个
            entry = min(usable, key=lambda e: e.last_used_at)
            entry.last_used_at = now
            logger.info(
                f"[CookiePool:{self.service}] 所有 Cookie 在冷却中，"
                f"使用最近最少使用的 Cookie (指纹: {entry.fingerprint})"
            )
            self._save()
            return entry.cookie

    async def get_pool_status(self) -> dict:
        """返回池状态快照"""
        async with self._lock:
            total = len(self._cookies)
            healthy = sum(1 for c in self._cookies if c.status == "healthy")
            suspect = sum(1 for c in self._cookies if c.status == "suspect")
            dead = sum(1 for c in self._cookies if c.status == "dead")
            return {
                "service": self.service,
                "total": total,
                "healthy": healthy,
                "suspect": suspect,
                "dead": dead,
                "cookies": [
                    {
                        "fingerprint": c.fingerprint,
                        "source": c.source,
                        "status": c.status,
                        "success_count": c.success_count,
                        "fail_count": c.fail_count,
                        "consecutive_fails": c.consecutive_fails,
                        "weight": round(c.weight, 2),
                        "last_used_at": c.last_used_at,
                        "added_at": c.added_at,
                    }
                    for c in self._cookies
                ],
            }

    # ------------------------------------------------------------------
    # Cookie 管理
    # ------------------------------------------------------------------

    async def add_cookie(self, cookie: str, source: str = "manual") -> bool:
        """
        向池中添加 Cookie。

        Args:
            cookie: Cookie 字符串
            source: 来源标识 (manual / browser / webhook / config)

        Returns:
            是否成功添加（去重会返回 False）
        """
        cookie = cookie.strip()
        if not cookie:
            return False

        async with self._lock:
            # 去重
            fingerprint = hashlib.md5(cookie.encode()).hexdigest()[:8]
            for entry in self._cookies:
                if entry.fingerprint == fingerprint:
                    logger.info(f"[CookiePool:{self.service}] Cookie 已存在 (指纹: {fingerprint})，跳过")
                    return False

            # 池容量限制
            if len(self._cookies) >= self.MAX_POOL_SIZE:
                # 移除最旧的 dead/suspect Cookie
                dead_or_suspect = [c for c in self._cookies if c.status != "healthy"]
                if dead_or_suspect:
                    to_remove = min(dead_or_suspect, key=lambda c: c.last_used_at)
                    self._cookies.remove(to_remove)
                    logger.info(
                        f"[CookiePool:{self.service}] 池已满，移除旧 {to_remove.status} Cookie "
                        f"(指纹: {to_remove.fingerprint})"
                    )
                else:
                    logger.warning(f"[CookiePool:{self.service}] 池已满且所有 Cookie 健康，拒绝添加")
                    return False

            entry = CookieEntry(cookie=cookie, source=source)
            self._cookies.append(entry)
            self._save()
            logger.info(
                f"[CookiePool:{self.service}] 新增 Cookie (指纹: {fingerprint}, 来源: {source}), "
                f"池大小: {len(self._cookies)}"
            )
            return True

    async def mark_success(self, cookie: str):
        """标记 Cookie 使用成功"""
        fingerprint = hashlib.md5(cookie.encode()).hexdigest()[:8]
        async with self._lock:
            for entry in self._cookies:
                if entry.fingerprint == fingerprint:
                    entry.success_count += 1
                    entry.consecutive_fails = 0
                    # 成功一次就恢复到 healthy
                    if entry.status == "suspect":
                        entry.status = "healthy"
                        logger.info(
                            f"[CookiePool:{self.service}] Cookie 恢复健康 (指纹: {fingerprint})"
                        )
                    # 调整权重
                    entry.weight = min(entry.weight * 1.1, 5.0)
                    self._save()
                    return
        logger.warning(
            f"[CookiePool:{self.service}] 未找到匹配 Cookie 以标记成功 (指纹: {fingerprint})"
        )

    async def mark_failure(self, cookie: str, reason: str = ""):
        """
        标记 Cookie 使用失败，自动降级。

        Args:
            cookie: Cookie 字符串
            reason: 失败原因（用于日志）
        """
        fingerprint = hashlib.md5(cookie.encode()).hexdigest()[:8]
        async with self._lock:
            for entry in self._cookies:
                if entry.fingerprint == fingerprint:
                    entry.fail_count += 1
                    entry.consecutive_fails += 1
                    entry.weight = max(entry.weight * 0.5, 0.1)

                    if entry.consecutive_fails >= self.DEAD_THRESHOLD:
                        entry.status = "dead"
                        logger.error(
                            f"[CookiePool:{self.service}] Cookie 已标记为 dead "
                            f"(指纹: {fingerprint}, 连续失败: {entry.consecutive_fails}, "
                            f"原因: {reason})"
                        )
                    elif entry.consecutive_fails >= self.SUSPECT_THRESHOLD:
                        entry.status = "suspect"
                        logger.warning(
                            f"[CookiePool:{self.service}] Cookie 已标记为 suspect "
                            f"(指纹: {fingerprint}, 连续失败: {entry.consecutive_fails}, "
                            f"原因: {reason})"
                        )

                    self._save()
                    return
        logger.warning(
            f"[CookiePool:{self.service}] 未找到匹配 Cookie 以标记失败 (指纹: {fingerprint})"
        )

    async def remove_cookie(self, fingerprint: str) -> bool:
        """通过指纹移除 Cookie"""
        async with self._lock:
            for i, entry in enumerate(self._cookies):
                if entry.fingerprint == fingerprint:
                    self._cookies.pop(i)
                    self._save()
                    logger.info(
                        f"[CookiePool:{self.service}] 已移除 Cookie (指纹: {fingerprint})"
                    )
                    return True
        return False

    async def cleanup_dead(self) -> int:
        """清理所有 dead 状态的 Cookie，返回清理数量"""
        async with self._lock:
            before = len(self._cookies)
            self._cookies = [c for c in self._cookies if c.status != "dead"]
            after = len(self._cookies)
            removed = before - after
            if removed > 0:
                self._save()
                logger.info(
                    f"[CookiePool:{self.service}] 清理了 {removed} 个 dead Cookie, "
                    f"剩余 {after} 个"
                )
            return removed

    async def clear_all(self) -> int:
        """清空池中所有 Cookie，返回清理数量"""
        async with self._lock:
            count = len(self._cookies)
            self._cookies.clear()
            self._save()
            logger.info(
                f"[CookiePool:{self.service}] 已清空全部 {count} 个 Cookie"
            )
            return count

    # ------------------------------------------------------------------
    # 自动获取
    # ------------------------------------------------------------------

    async def auto_extract_from_browser(self) -> int:
        """
        从本地浏览器自动提取 Cookie（使用 browser_cookie3）。

        Returns:
            成功提取的 Cookie 数量
        """
        try:
            import browser_cookie3
        except ImportError:
            logger.warning("[CookiePool] browser-cookie3 未安装，跳过浏览器提取")
            return 0

        # 各平台的域名映射
        domain_map = {
            "douyin": "douyin.com",
            "tiktok": "tiktok.com",
        }
        domain = domain_map.get(self.service, f"{self.service}.com")

        added_count = 0
        browser_funcs = [
            ("Chrome", browser_cookie3.chrome),
            ("Firefox", browser_cookie3.firefox),
            ("Edge", browser_cookie3.edge),
            ("Chromium", browser_cookie3.chromium),
            ("Opera", browser_cookie3.opera),
            ("Brave", browser_cookie3.brave),
        ]

        for browser_name, browser_fn in browser_funcs:
            try:
                cj = browser_fn(domain_name=domain)
                cookie_parts = []
                for cookie_obj in cj:
                    if cookie_obj.name and cookie_obj.value:
                        cookie_parts.append(f"{cookie_obj.name}={cookie_obj.value}")

                if cookie_parts:
                    cookie_str = "; ".join(cookie_parts)
                    if await self.add_cookie(cookie_str, source=f"browser:{browser_name}"):
                        added_count += 1
                        logger.info(
                            f"[CookiePool:{self.service}] 从 {browser_name} 提取到 Cookie "
                            f"({len(cookie_parts)} 个键值对)"
                        )
            except Exception as e:
                logger.debug(
                    f"[CookiePool:{self.service}] 从 {browser_name} 提取失败: {e}"
                )

        return added_count

    # ------------------------------------------------------------------
    # 健康检查
    # ------------------------------------------------------------------

    async def health_check(self):
        """后台探活：标记距离上次检查超过 HEALTH_CHECK_INTERVAL 的 Cookie"""
        now = time.time()
        if now - self._last_health_check < self.HEALTH_CHECK_INTERVAL:
            return

        self._last_health_check = now
        logger.info(f"[CookiePool:{self.service}] 执行健康检查，池大小: {len(self._cookies)}")

        # 健康检查不主动标记失败，只做统计记录
        # 实际的失效检测由 BaseCrawler 在请求响应中完成
        async with self._lock:
            healthy = sum(1 for c in self._cookies if c.status == "healthy")
            suspect = sum(1 for c in self._cookies if c.status == "suspect")
            dead = sum(1 for c in self._cookies if c.status == "dead")
            logger.info(
                f"[CookiePool:{self.service}] 健康状态: healthy={healthy}, "
                f"suspect={suspect}, dead={dead}"
            )

    # ------------------------------------------------------------------
    # 向后兼容：从 config.yaml 种子
    # ------------------------------------------------------------------

    def seed_from_config(self, config: dict, config_path: str):
        """
        从 config.yaml 读取 Cookie 作为种子（同步方法，应在初始化时调用）。

        Args:
            config: 已加载的 YAML 配置字典
            config_path: config.yaml 的文件路径（用于日志）
        """
        self._config_path = Path(config_path)
        try:
            if self.service == "douyin":
                cookie = config.get("TokenManager", {}).get("douyin", {}).get("headers", {}).get("Cookie", "")
            elif self.service == "tiktok":
                cookie = config.get("TokenManager", {}).get("tiktok", {}).get("headers", {}).get("Cookie", "")
            else:
                return

            cookie = cookie.strip()
            if not cookie:
                return

            # 同步添加（在模块加载阶段，还没有事件循环）
            fingerprint = hashlib.md5(cookie.encode()).hexdigest()[:8]
            if not any(c.fingerprint == fingerprint for c in self._cookies):
                entry = CookieEntry(cookie=cookie, source="config")
                self._cookies.append(entry)
                self._save()
                logger.info(
                    f"[CookiePool:{self.service}] 从 config.yaml 种子 Cookie "
                    f"(指纹: {fingerprint}), 池大小: {len(self._cookies)}"
                )
        except Exception as e:
            logger.warning(f"[CookiePool:{self.service}] 从 config 种子失败: {e}")

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------

    def _load(self):
        """从 JSON 文件加载 Cookie 池"""
        if not self._store_path.exists():
            logger.info(f"[CookiePool:{self.service}] 存储文件不存在，将创建新池")
            return

        try:
            with open(self._store_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            entries = data.get("cookies", [])
            for raw in entries:
                entry = CookieEntry(
                    cookie=raw.get("cookie", ""),
                    source=raw.get("source", "unknown"),
                    added_at=raw.get("added_at", 0.0),
                    last_used_at=raw.get("last_used_at", 0.0),
                    success_count=raw.get("success_count", 0),
                    fail_count=raw.get("fail_count", 0),
                    consecutive_fails=raw.get("consecutive_fails", 0),
                    weight=raw.get("weight", 1.0),
                    status=raw.get("status", "healthy"),
                )
                if entry.cookie:
                    self._cookies.append(entry)

            logger.info(
                f"[CookiePool:{self.service}] 从存储加载了 {len(self._cookies)} 个 Cookie"
            )
        except Exception as e:
            logger.error(f"[CookiePool:{self.service}] 加载存储失败: {e}")

    def _save(self):
        """持久化到 JSON 文件"""
        try:
            data = {
                "service": self.service,
                "updated_at": time.time(),
                "cookies": [asdict(c) for c in self._cookies],
            }
            # 原子写入：先写临时文件，再 rename
            tmp_path = self._store_path.with_suffix(".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, self._store_path)
        except Exception as e:
            logger.error(f"[CookiePool:{self.service}] 保存存储失败: {e}")


# ==============================================================================
# 便捷函数
# ==============================================================================


def is_cookie_failure_response(status_code: int, response_body: str = "") -> bool:
    """
    判断响应是否表明 Cookie 失效。

    检测条件（严格模式，避免误杀健康 Cookie）:
    - HTTP 401: 明确未授权，Cookie 一定失效
    - HTTP 403 + 风控验证特征: captcha / 滑块 / 验证码（而非泛化的 "登录" 提示）

    注意：不匹配 "login" / "登录" / "sign in" 等宽泛关键词，
    因为正常接口也可能返回 "请登录" 的 403 提示而 Cookie 本身是有效的。

    Args:
        status_code: HTTP 状态码
        response_body: 响应体文本

    Returns:
        是否可能是 Cookie 失效
    """
    if status_code == 401:
        return True

    if status_code == 403:
        body_lower = response_body.lower() if response_body else ""
        # 仅匹配风控验证特征，不匹配泛化的 "登录" 提示
        risk_indicators = [
            "captcha", "滑块", "verify", "验证码",
            "slide", "security", "风控",
        ]
        if any(indicator in body_lower for indicator in risk_indicators):
            return True

    return False

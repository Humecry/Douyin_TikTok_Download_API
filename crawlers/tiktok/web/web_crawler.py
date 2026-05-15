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
# 　　　　 　　  ＿＿
# 　　　 　　 ／＞　　フ
# 　　　 　　| 　_　 _ l
# 　 　　 　／` ミ＿xノ
# 　　 　 /　　　 　 |       Feed me Stars ⭐ ️
# 　　　 /　 ヽ　　 ﾉ
# 　 　 │　　|　|　|
# 　／￣|　　 |　|　|
# 　| (￣ヽ＿_ヽ_)__)
# 　＼二つ
# ==============================================================================
#
# Contributor Link:
# - https://github.com/Evil0ctal
# - https://github.com/Johnserf-Seed
#
# ==============================================================================


import asyncio  # 异步I/O
import time  # 时间操作
import yaml  # 配置文件
import os  # 系统操作

# 基础爬虫客户端和TikTokAPI端点
from crawlers.base_crawler import BaseCrawler, CookieFailureError
from crawlers.tiktok.web.endpoints import TikTokAPIEndpoints
from crawlers.utils.utils import extract_valid_urls

# TikTok加密参数生成器
from crawlers.tiktok.web.utils import (
    AwemeIdFetcher,
    BogusManager,
    SecUserIdFetcher,
    TokenManager
)

# TikTok接口数据请求模型
from crawlers.tiktok.web.models import (
    UserProfile,
    UserPost,
    UserLike,
    UserMix,
    UserCollect,
    PostDetail,
    UserPlayList,
    PostComment,
    PostCommentReply,
    UserFans,
    UserFollow
)

from crawlers.utils.cookie_manager import CookiePool
from crawlers.utils.logger import logger


# 配置文件路径
path = os.path.abspath(os.path.dirname(__file__))

# 读取配置文件
with open(f"{path}/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Cookie 池单例（模块级，全局共享）
_cookie_pool = CookiePool.get_instance("tiktok")
# 从 config.yaml 种子（向后兼容）
_cookie_pool.seed_from_config(config, f"{path}/config.yaml")

# Cookie 轮转最大尝试次数
COOKIE_RETRY_MAX = 3


class TikTokWebCrawler:

    def __init__(self):
        self.proxy_pool = None
        self.cookie_pool = _cookie_pool

    # 从 Cookie 池获取请求头
    async def get_tiktok_headers(self, cookie: str = None):
        """
        获取TikTok请求头。

        优先使用传入的 cookie，否则从 Cookie 池获取。
        """
        tiktok_config = config["TokenManager"]["tiktok"]

        # 如果没有传入 cookie，从池中获取
        if cookie is None:
            cookie = await self.cookie_pool.get_cookie()

        kwargs = {
            "headers": {
                "User-Agent": tiktok_config["headers"]["User-Agent"],
                "Referer": tiktok_config["headers"]["Referer"],
                "Cookie": cookie,
            },
            "proxies": {"http://": tiktok_config["proxies"]["http"],
                        "https://": tiktok_config["proxies"]["https"]}
        }
        return kwargs

    # Cookie 感知的请求包装器
    async def _fetch_with_cookie_rotation(self, fetch_fn, *args, **kwargs):
        """
        用 Cookie 轮转包装一个异步请求函数。

        流程:
        1. 从池获取 Cookie → 构建 headers → 创建 BaseCrawler
        2. 执行请求
        3. 若触发 CookieFailureError → 标记失败 → 换 Cookie 重试
        4. 成功 → 标记成功 → 返回结果
        """
        last_error = None

        for attempt in range(COOKIE_RETRY_MAX):
            headers_kwargs = await self.get_tiktok_headers()
            current_cookie = headers_kwargs["headers"]["Cookie"]

            base_crawler = BaseCrawler(
                proxies=headers_kwargs["proxies"],
                crawler_headers=headers_kwargs["headers"],
                cookie_str=current_cookie,
            )

            try:
                async with base_crawler as crawler:
                    result = await fetch_fn(crawler, headers_kwargs, *args, **kwargs)

                await self.cookie_pool.mark_success(current_cookie)
                return result

            except CookieFailureError as e:
                logger.warning(
                    f"[TikTokWebCrawler] Cookie 失效 (尝试 {attempt + 1}/{COOKIE_RETRY_MAX}), "
                    f"切换 Cookie 重试. 原因: {e.message}"
                )
                await self.cookie_pool.mark_failure(
                    current_cookie, reason=str(e.message)
                )
                last_error = e
                continue

            except Exception:
                raise

        raise CookieFailureError(
            message=f"所有 Cookie 均已尝试 ({COOKIE_RETRY_MAX} 次)，请求失败",
            status_code=getattr(last_error, 'status_code', None),
        )

    """-------------------------------------------------------handler接口列表-------------------------------------------------------"""

    # 获取单个作品数据
    async def fetch_one_video(self, itemId: str):
        async def _fetch(crawler, headers_kwargs):
            params = PostDetail(itemId=itemId)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.POST_DETAIL, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户的个人信息
    async def fetch_user_profile(self, secUid: str, uniqueId: str):
        async def _fetch(crawler, headers_kwargs):
            params = UserProfile(secUid=secUid, uniqueId=uniqueId)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.USER_DETAIL, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户的作品列表
    async def fetch_user_post(self, secUid: str, cursor: int = 0, count: int = 35, coverFormat: int = 2):
        async def _fetch(crawler, headers_kwargs):
            params = UserPost(secUid=secUid, cursor=cursor, count=count, coverFormat=coverFormat)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.USER_POST, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户的点赞列表
    async def fetch_user_like(self, secUid: str, cursor: int = 0, count: int = 30, coverFormat: int = 2):
        async def _fetch(crawler, headers_kwargs):
            params = UserLike(secUid=secUid, cursor=cursor, count=count, coverFormat=coverFormat)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.USER_LIKE, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户的收藏列表（用户提供自己的Cookie）
    async def fetch_user_collect(self, cookie: str, secUid: str, cursor: int = 0, count: int = 30,
                                  coverFormat: int = 2):
        # 用户提供自己的 Cookie 时不使用池轮转
        headers_kwargs = await self.get_tiktok_headers(cookie=cookie)
        base_crawler = BaseCrawler(
            proxies=headers_kwargs["proxies"],
            crawler_headers=headers_kwargs["headers"],
            cookie_str=cookie,
        )
        async with base_crawler as crawler:
            params = UserCollect(cookie=cookie, secUid=secUid, cursor=cursor, count=count, coverFormat=coverFormat)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.USER_COLLECT, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

    # 获取用户的播放列表
    async def fetch_user_play_list(self, secUid: str, cursor: int = 0, count: int = 30):
        async def _fetch(crawler, headers_kwargs):
            params = UserPlayList(secUid=secUid, cursor=cursor, count=count)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.USER_PLAY_LIST, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户的合辑列表
    async def fetch_user_mix(self, mixId: str, cursor: int = 0, count: int = 30):
        async def _fetch(crawler, headers_kwargs):
            params = UserMix(mixId=mixId, cursor=cursor, count=count)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.USER_MIX, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取作品的评论列表
    async def fetch_post_comment(self, aweme_id: str, cursor: int = 0, count: int = 20, current_region: str = ""):
        async def _fetch(crawler, headers_kwargs):
            params = PostComment(aweme_id=aweme_id, cursor=cursor, count=count, current_region=current_region)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.POST_COMMENT, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取作品的评论回复列表
    async def fetch_post_comment_reply(self, item_id: str, comment_id: str, cursor: int = 0, count: int = 20,
                                        current_region: str = ""):
        async def _fetch(crawler, headers_kwargs):
            params = PostCommentReply(item_id=item_id, comment_id=comment_id, cursor=cursor, count=count,
                                       current_region=current_region)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.POST_COMMENT_REPLY, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户的粉丝列表
    async def fetch_user_fans(self, secUid: str, count: int = 30, maxCursor: int = 0, minCursor: int = 0):
        async def _fetch(crawler, headers_kwargs):
            params = UserFans(secUid=secUid, count=count, maxCursor=maxCursor, minCursor=minCursor)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.USER_FANS, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户的关注列表
    async def fetch_user_follow(self, secUid: str, count: int = 30, maxCursor: int = 0, minCursor: int = 0):
        async def _fetch(crawler, headers_kwargs):
            params = UserFollow(secUid=secUid, count=count, maxCursor=maxCursor, minCursor=minCursor)
            endpoint = BogusManager.model_2_endpoint(
                TikTokAPIEndpoints.USER_FOLLOW, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    """-------------------------------------------------------utils接口列表-------------------------------------------------------"""

    # 生成真实msToken
    async def fetch_real_msToken(self):
        result = {
            "msToken": TokenManager().gen_real_msToken()
        }
        return result

    # 生成ttwid
    async def gen_ttwid(self, cookie: str):
        result = {
            "ttwid": TokenManager().gen_ttwid(cookie)
        }
        return result

    # 生成xbogus
    async def gen_xbogus(self, url: str, user_agent: str):
        url = BogusManager.xb_str_2_endpoint(user_agent, url)
        result = {
            "url": url,
            "x_bogus": url.split("&X-Bogus=")[1],
            "user_agent": user_agent
        }
        return result

    # 提取单个用户id
    async def get_sec_user_id(self, url: str):
        return await SecUserIdFetcher.get_secuid(url)

    # 提取列表用户id
    async def get_all_sec_user_id(self, urls: list):
        # 提取有效URL
        urls = extract_valid_urls(urls)

        # 对于URL列表
        return await SecUserIdFetcher.get_all_secuid(urls)

    # 提取单个作品id
    async def get_aweme_id(self, url: str):
        return await AwemeIdFetcher.get_aweme_id(url)

    # 提取列表作品id
    async def get_all_aweme_id(self, urls: list):
        # 提取有效URL
        urls = extract_valid_urls(urls)

        # 对于URL列表
        return await AwemeIdFetcher.get_all_aweme_id(urls)

    # 获取用户unique_id
    async def get_unique_id(self, url: str):
        return await SecUserIdFetcher.get_uniqueid(url)

    # 获取列表unique_id列表
    async def get_all_unique_id(self, urls: list):
        # 提取有效URL
        urls = extract_valid_urls(urls)

        # 对于URL列表
        return await SecUserIdFetcher.get_all_uniqueid(urls)

    async def update_cookie(self, cookie: str):
        """
        将新 Cookie 加入池中。

        Args:
            cookie: 新的Cookie值
        """
        logger.info(f"[TikTokWebCrawler] 收到 Cookie 更新请求，向池中添加")
        added = await self.cookie_pool.add_cookie(cookie, source="webhook")
        if added:
            logger.info("[TikTokWebCrawler] Cookie 已入池")
        else:
            logger.info("[TikTokWebCrawler] Cookie 已存在或池满，跳过")
        return {"added": added, "pool_status": await self.cookie_pool.get_pool_status()}

    # 获取 Cookie 池状态
    async def get_cookie_pool_status(self):
        return await self.cookie_pool.get_pool_status()

    """-------------------------------------------------------main接口列表-------------------------------------------------------"""

    async def main(self):
        # 占位
        pass


if __name__ == "__main__":
    # 初始化
    TikTokWebCrawler = TikTokWebCrawler()

    # 开始时间
    start = time.time()

    asyncio.run(TikTokWebCrawler.main())

    # 结束时间
    end = time.time()
    print(f"耗时：{end - start}")

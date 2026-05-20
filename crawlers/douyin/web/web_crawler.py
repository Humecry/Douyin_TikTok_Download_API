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
import os  # 系统操作
import time  # 时间操作
from urllib.parse import urlencode, quote  # URL编码
import yaml  # 配置文件

# 基础爬虫客户端和抖音API端点
from crawlers.base_crawler import BaseCrawler, CookieFailureError
from crawlers.douyin.web.endpoints import DouyinAPIEndpoints
# 抖音接口数据请求模型
from crawlers.douyin.web.models import (
    BaseRequestModel, LiveRoomRanking, PostComments,
    PostCommentsReply, PostDetail,
    UserProfile, UserCollection, UserLike, UserLive,
    UserLive2, UserMix, UserPost
)
# 抖音应用的工具类
from crawlers.douyin.web.utils import (AwemeIdFetcher,  # Aweme ID获取
                                       BogusManager,  # XBogus管理
                                       SecUserIdFetcher,  # 安全用户ID获取
                                       TokenManager,  # 令牌管理
                                       VerifyFpManager,  # 验证管理
                                       WebCastIdFetcher,  # 直播ID获取
                                       extract_valid_urls  # URL提取
                                       )

from crawlers.utils.cookie_manager import CookiePool
from crawlers.utils.logger import logger

# 配置文件路径
path = os.path.abspath(os.path.dirname(__file__))

# 读取配置文件
with open(f"{path}/config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Cookie 池单例（模块级，全局共享）
_cookie_pool = CookiePool.get_instance("douyin")
# 从 config.yaml 种子（已禁用——Cookie 完全由 API 手动管理）
# _cookie_pool.seed_from_config(config, f"{path}/config.yaml")  # 已禁用——Cookie 由 API 手动管理

# Cookie 轮转最大尝试次数
COOKIE_RETRY_MAX = 3


class DouyinWebCrawler:

    def __init__(self):
        self.cookie_pool = _cookie_pool

    # 从 Cookie 池获取请求头
    async def get_douyin_headers(self, cookie: str = None):
        """
        获取抖音请求头。

        优先使用传入的 cookie，否则从 Cookie 池获取。
        """
        douyin_config = config["TokenManager"]["douyin"]

        # 如果没有传入 cookie，从池中获取
        if cookie is None:
            cookie = await self.cookie_pool.get_cookie()

        kwargs = {
            "headers": {
                "Accept-Language": douyin_config["headers"]["Accept-Language"],
                "User-Agent": douyin_config["headers"]["User-Agent"],
                "Referer": douyin_config["headers"]["Referer"],
                "Cookie": cookie,
            },
            "proxies": {"http://": douyin_config["proxies"]["http"], "https://": douyin_config["proxies"]["https"]},
        }
        return kwargs

    # Cookie 感知的请求包装器
    async def _fetch_with_cookie_rotation(self, fetch_fn, *args, **kwargs):
        """
        用 Cookie 轮转包装一个异步请求函数。

        流程:
        1. 从池获取 Cookie → 构建 headers → 创建 BaseCrawler
        2. 执行请求
        3. 若触发 CookieFailureError → 标记失败 → 换 Cookie 重试 (最多 COOKIE_RETRY_MAX 次)
        4. 成功 → 标记成功 → 返回结果

        Args:
            fetch_fn: 异步函数，签名为 async def(crawler: BaseCrawler, headers: dict) -> result
        """
        last_error = None

        for attempt in range(COOKIE_RETRY_MAX):
            # 获取 headers（含 Cookie）
            headers_kwargs = await self.get_douyin_headers()
            current_cookie = headers_kwargs["headers"]["Cookie"]

            # 创建 BaseCrawler
            base_crawler = BaseCrawler(
                proxies=headers_kwargs["proxies"],
                crawler_headers=headers_kwargs["headers"],
                cookie_str=current_cookie,
            )

            try:
                async with base_crawler as crawler:
                    result = await fetch_fn(crawler, headers_kwargs, *args, **kwargs)

                # 成功：标记 Cookie
                await self.cookie_pool.mark_success(current_cookie)
                return result

            except CookieFailureError as e:
                logger.warning(
                    f"[DouyinWebCrawler] Cookie 失效 (尝试 {attempt + 1}/{COOKIE_RETRY_MAX}), "
                    f"切换 Cookie 重试. 原因: {e.message}"
                )
                await self.cookie_pool.mark_failure(
                    current_cookie, reason=str(e.message)
                )
                last_error = e
                # 继续下一个 Cookie
                continue

            except Exception as e:
                # 其他异常：不确定是否 Cookie 问题，不标记失败
                raise

        # 所有 Cookie 都失败
        raise CookieFailureError(
            message=f"所有 Cookie 均已尝试 ({COOKIE_RETRY_MAX} 次)，请求失败",
            status_code=getattr(last_error, 'status_code', None),
        )

    "-------------------------------------------------------handler接口列表-------------------------------------------------------"

    # 获取单个作品数据
    async def fetch_one_video(self, aweme_id: str):
        async def _fetch(crawler, headers_kwargs):
            params = PostDetail(aweme_id=aweme_id)
            params_dict = params.dict()
            params_dict["msToken"] = ''
            a_bogus = BogusManager.ab_model_2_endpoint(params_dict, headers_kwargs["headers"]["User-Agent"])
            endpoint = f"{DouyinAPIEndpoints.POST_DETAIL}?{urlencode(params_dict)}&a_bogus={a_bogus}"
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户发布作品数据
    async def fetch_user_post_videos(self, sec_user_id: str, max_cursor: int, count: int):
        async def _fetch(crawler, headers_kwargs):
            params = UserPost(sec_user_id=sec_user_id, max_cursor=max_cursor, count=count)
            params_dict = params.dict()
            params_dict["msToken"] = ''
            a_bogus = BogusManager.ab_model_2_endpoint(params_dict, headers_kwargs["headers"]["User-Agent"])
            endpoint = f"{DouyinAPIEndpoints.USER_POST}?{urlencode(params_dict)}&a_bogus={a_bogus}"
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户喜欢作品数据
    async def fetch_user_like_videos(self, sec_user_id: str, max_cursor: int, count: int):
        async def _fetch(crawler, headers_kwargs):
            params = UserLike(sec_user_id=sec_user_id, max_cursor=max_cursor, count=count)
            params_dict = params.dict()
            params_dict["msToken"] = ''
            a_bogus = BogusManager.ab_model_2_endpoint(params_dict, headers_kwargs["headers"]["User-Agent"])
            endpoint = f"{DouyinAPIEndpoints.USER_FAVORITE_A}?{urlencode(params_dict)}&a_bogus={a_bogus}"
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户收藏作品数据（用户提供自己的Cookie）
    async def fetch_user_collection_videos(self, cookie: str, cursor: int = 0, count: int = 20):
        async def _fetch(crawler, headers_kwargs):
            # 覆盖为调用方传入的 cookie
            headers_kwargs["headers"]["Cookie"] = cookie
            params = UserCollection(cursor=cursor, count=count)
            endpoint = BogusManager.xb_model_2_endpoint(
                DouyinAPIEndpoints.USER_COLLECTION, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            # 用户收藏接口较特殊：使用传入的 cookie，不参与池轮转
            return await crawler.fetch_post_json(endpoint)

        # 用户提供自己的 Cookie 时不使用池轮转
        headers_kwargs = await self.get_douyin_headers(cookie=cookie)
        base_crawler = BaseCrawler(
            proxies=headers_kwargs["proxies"],
            crawler_headers=headers_kwargs["headers"],
            cookie_str=cookie,
        )
        async with base_crawler as crawler:
            return await _fetch(crawler, headers_kwargs)

    # 获取用户合辑作品数据
    async def fetch_user_mix_videos(self, mix_id: str, cursor: int = 0, count: int = 20):
        async def _fetch(crawler, headers_kwargs):
            params = UserMix(mix_id=mix_id, cursor=cursor, count=count)
            endpoint = BogusManager.xb_model_2_endpoint(
                DouyinAPIEndpoints.MIX_AWEME, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取用户直播流数据
    async def fetch_user_live_videos(self, webcast_id: str, room_id_str=""):
        async def _fetch(crawler, headers_kwargs):
            params = UserLive(web_rid=webcast_id, room_id_str=room_id_str)
            endpoint = BogusManager.xb_model_2_endpoint(
                DouyinAPIEndpoints.LIVE_INFO, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取指定用户的直播流数据
    async def fetch_user_live_videos_by_room_id(self, room_id: str):
        async def _fetch(crawler, headers_kwargs):
            params = UserLive2(room_id=room_id)
            endpoint = BogusManager.xb_model_2_endpoint(
                DouyinAPIEndpoints.LIVE_INFO_ROOM_ID, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取直播间送礼用户排行榜
    async def fetch_live_gift_ranking(self, room_id: str, rank_type: int = 30):
        async def _fetch(crawler, headers_kwargs):
            params = LiveRoomRanking(room_id=room_id, rank_type=rank_type)
            endpoint = BogusManager.xb_model_2_endpoint(
                DouyinAPIEndpoints.LIVE_GIFT_RANK, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取指定用户的信息
    async def handler_user_profile(self, sec_user_id: str):
        async def _fetch(crawler, headers_kwargs):
            params = UserProfile(sec_user_id=sec_user_id)
            endpoint = BogusManager.xb_model_2_endpoint(
                DouyinAPIEndpoints.USER_DETAIL, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取指定视频的评论数据
    async def fetch_video_comments(self, aweme_id: str, cursor: int = 0, count: int = 20):
        async def _fetch(crawler, headers_kwargs):
            params = PostComments(aweme_id=aweme_id, cursor=cursor, count=count)
            endpoint = BogusManager.xb_model_2_endpoint(
                DouyinAPIEndpoints.POST_COMMENT, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取指定视频的评论回复数据
    async def fetch_video_comments_reply(self, item_id: str, comment_id: str, cursor: int = 0, count: int = 20):
        async def _fetch(crawler, headers_kwargs):
            params = PostCommentsReply(item_id=item_id, comment_id=comment_id, cursor=cursor, count=count)
            endpoint = BogusManager.xb_model_2_endpoint(
                DouyinAPIEndpoints.POST_COMMENT_REPLY, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    # 获取抖音热榜数据
    async def fetch_hot_search_result(self):
        async def _fetch(crawler, headers_kwargs):
            params = BaseRequestModel()
            endpoint = BogusManager.xb_model_2_endpoint(
                DouyinAPIEndpoints.DOUYIN_HOT_SEARCH, params.dict(), headers_kwargs["headers"]["User-Agent"]
            )
            return await crawler.fetch_get_json(endpoint)

        return await self._fetch_with_cookie_rotation(_fetch)

    "-------------------------------------------------------utils接口列表-------------------------------------------------------"

    # 生成真实msToken
    async def gen_real_msToken(self, ):
        result = {
            "msToken": TokenManager().gen_real_msToken()
        }
        return result

    # 生成ttwid
    async def gen_ttwid(self, ):
        result = {
            "ttwid": TokenManager().gen_ttwid()
        }
        return result

    # 生成verify_fp
    async def gen_verify_fp(self, ):
        result = {
            "verify_fp": VerifyFpManager.gen_verify_fp()
        }
        return result

    # 生成s_v_web_id
    async def gen_s_v_web_id(self, ):
        result = {
            "s_v_web_id": VerifyFpManager.gen_s_v_web_id()
        }
        return result

    # 使用接口地址生成Xb参数
    async def get_x_bogus(self, url: str, user_agent: str):
        url = BogusManager.xb_str_2_endpoint(url, user_agent)
        result = {
            "url": url,
            "x_bogus": url.split("&X-Bogus=")[1],
            "user_agent": user_agent
        }
        return result

    # 使用接口地址生成Ab参数
    async def get_a_bogus(self, url: str, user_agent: str):
        endpoint = url.split("?")[0]
        # 将URL参数转换为dict
        params = dict([i.split("=") for i in url.split("?")[1].split("&")])
        # 去除URL中的msToken参数
        params["msToken"] = ""
        a_bogus = BogusManager.ab_model_2_endpoint(params, user_agent)
        result = {
            "url": f"{endpoint}?{urlencode(params)}&a_bogus={a_bogus}",
            "a_bogus": a_bogus,
            "user_agent": user_agent
        }
        return result

    # 提取单个用户id
    async def get_sec_user_id(self, url: str):
        return await SecUserIdFetcher.get_sec_user_id(url)

    # 提取列表用户id
    async def get_all_sec_user_id(self, urls: list):
        # 提取有效URL
        urls = extract_valid_urls(urls)

        # 对于URL列表
        return await SecUserIdFetcher.get_all_sec_user_id(urls)

    # 提取单个作品id
    async def get_aweme_id(self, url: str):
        return await AwemeIdFetcher.get_aweme_id(url)

    # 提取列表作品id
    async def get_all_aweme_id(self, urls: list):
        # 提取有效URL
        urls = extract_valid_urls(urls)

        # 对于URL列表
        return await AwemeIdFetcher.get_all_aweme_id(urls)

    # 提取单个直播间号
    async def get_webcast_id(self, url: str):
        return await WebCastIdFetcher.get_webcast_id(url)

    # 提取列表直播间号
    async def get_all_webcast_id(self, urls: list):
        # 提取有效URL
        urls = extract_valid_urls(urls)

        # 对于URL列表
        return await WebCastIdFetcher.get_all_webcast_id(urls)

    async def update_cookie(self, cookie: str):
        """
        将新 Cookie 加入池中（替代旧的单 Cookie 覆盖模式）。

        Args:
            cookie: 新的Cookie值
        """
        logger.info(f"[DouyinWebCrawler] 收到 Cookie 更新请求，向池中添加")
        added = await self.cookie_pool.add_cookie(cookie, source="webhook")
        if added:
            logger.info("[DouyinWebCrawler] Cookie 已入池")
        else:
            logger.info("[DouyinWebCrawler] Cookie 已存在或池满，跳过")
        return {"added": added, "pool_status": await self.cookie_pool.get_pool_status()}

    # 获取 Cookie 池状态（新增接口）
    async def get_cookie_pool_status(self):
        """返回当前的 Cookie 池状态"""
        return await self.cookie_pool.get_pool_status()

    # 从浏览器自动提取 Cookie（新增接口）
    async def extract_browser_cookies(self):
        """从本地浏览器自动提取 Cookie"""
        count = await self.cookie_pool.auto_extract_from_browser()
        return {"extracted": count, "pool_status": await self.cookie_pool.get_pool_status()}

    async def main(self):
        """-------------------------------------------------------handler接口列表-------------------------------------------------------"""

        # 获取单一视频信息
        # aweme_id = "7372484719365098803"
        # result = await self.fetch_one_video(aweme_id)
        # print(result)

        # 获取用户发布作品数据
        # sec_user_id = "MS4wLjABAAAANXSltcLCzDGmdNFI2Q_QixVTr67NiYzjKOIP5s03CAE"
        # max_cursor = 0
        # count = 10
        # result = await self.fetch_user_post_videos(sec_user_id, max_cursor, count)
        # print(result)

        # 占位
        pass


if __name__ == "__main__":
    # 初始化
    DouyinWebCrawler = DouyinWebCrawler()

    # 开始时间
    start = time.time()

    asyncio.run(DouyinWebCrawler.main())

    # 结束时间
    end = time.time()
    print(f"耗时：{end - start}")

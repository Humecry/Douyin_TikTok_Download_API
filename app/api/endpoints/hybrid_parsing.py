import asyncio

from fastapi import APIRouter, Body, Query, Request, HTTPException  # 导入FastAPI组件

from app.api.models.APIResponseModel import ResponseModel, ErrorResponseModel  # 导入响应模型

# 爬虫/Crawler
from crawlers.hybrid.hybrid_crawler import HybridCrawler  # 导入混合爬虫
from crawlers.utils.cookie_manager import CookiePool

HybridCrawler = HybridCrawler()  # 实例化混合爬虫

router = APIRouter()


@router.get("/video_data", response_model=ResponseModel, tags=["Hybrid-API"],
            summary="混合解析单一视频接口/Hybrid parsing single video endpoint")
async def hybrid_parsing_single_video(request: Request,
                                      url: str = Query(example="https://v.douyin.com/L4FJNR3/"),
                                      minimal: bool = Query(default=False)):
    """
    # [中文]
    ### 用途:
    - 该接口用于解析抖音/TikTok单一视频的数据。
    ### 参数:
    - `url`: 视频链接、分享链接、分享文本。
    ### 返回:
    - `data`: 视频数据。

    # [English]
    ### Purpose:
    - This endpoint is used to parse data of a single Douyin/TikTok video.
    ### Parameters:
    - `url`: Video link, share link, or share text.
    ### Returns:
    - `data`: Video data.

    # [Example]
    url = "https://v.douyin.com/L4FJNR3/"
    """
    try:
        # 解析视频/Parse video
        data = await HybridCrawler.hybrid_parsing_single_video(url=url, minimal=minimal)
        # 返回数据/Return data
        return ResponseModel(code=200,
                             router=request.url.path,
                             data=data)
    except Exception as e:
        status_code = 400
        detail = ErrorResponseModel(code=status_code,
                                    router=request.url.path,
                                    params=dict(request.query_params),
                                    )
        raise HTTPException(status_code=status_code, detail=detail.dict())


# 更新Cookie（入池模式：不再替换单 Cookie，而是加入 Cookie 池统一管理）
@router.post("/update_cookie",
             response_model=ResponseModel,
             summary="更新Cookie/Update Cookie (入池)")
async def update_cookie_api(request: Request,
                           service: str = Body(example="douyin", description="服务名称/Service name"),
                           cookie: str = Body(example="YOUR_NEW_COOKIE", description="新的Cookie值/New Cookie value")):
    """
    # [中文]
    ### 用途:
    - 将新的 Cookie 加入指定服务的 Cookie 池。
    - 系统会自动管理池中的多个 Cookie，按成功率加权轮转，失效自动降级。
    ### 参数:
    - service: 服务名称 (douyin / tiktok)
    - cookie: 新的Cookie值
    ### 返回:
    - 入池结果及池状态

    # [English]
    ### Purpose:
    - Add a new Cookie to the Cookie pool for the specified service.
    - The system automatically manages multiple cookies with weighted rotation and auto-degradation.
    ### Parameters:
    - service: Service name (douyin / tiktok)
    - cookie: New Cookie value
    ### Return:
    - Pool add result and pool status

    # [示例/Example]
    service = "douyin"
    cookie = "YOUR_NEW_COOKIE"
    """
    try:
        if service not in ("douyin", "tiktok"):
            raise ValueError(f"不支持的服务 '{service}'。支持的服务: douyin, tiktok")

        pool = CookiePool.get_instance(service)
        added = await pool.add_cookie(cookie, source="api")
        status = await pool.get_pool_status()

        return ResponseModel(
            code=200,
            router=request.url.path,
            data={
                "message": f"Cookie 已{'加入' if added else '跳过（已存在或池满）'} {service} 池",
                "added": added,
                "pool_status": status,
            }
        )
    except Exception as e:
        status_code = 400
        detail = ErrorResponseModel(code=status_code,
                                    router=request.url.path,
                                    params=dict(request.query_params),
                                    )
        raise HTTPException(status_code=status_code, detail=detail.dict())


# 获取 Cookie 池状态
@router.get("/cookie_pool_status",
            response_model=ResponseModel,
            summary="获取Cookie池状态/Get Cookie pool status")
async def cookie_pool_status_api(request: Request,
                                  service: str = Query(default="douyin", description="服务名称/Service name")):
    """
    # [中文]
    ### 用途:
    - 查看指定服务的 Cookie 池状态，包括每个 Cookie 的成功/失败次数、权重等。
    ### 参数:
    - service: 服务名称 (douyin / tiktok)
    ### 返回:
    - 池状态快照

    # [English]
    ### Purpose:
    - View the Cookie pool status for the specified service.
    ### Parameters:
    - service: Service name (douyin / tiktok)
    ### Return:
    - Pool status snapshot
    """
    try:
        if service not in ("douyin", "tiktok"):
            raise ValueError(f"不支持的服务 '{service}'。支持的服务: douyin, tiktok")

        pool = CookiePool.get_instance(service)
        status = await pool.get_pool_status()
        return ResponseModel(code=200,
                             router=request.url.path,
                             data=status)
    except Exception as e:
        status_code = 400
        detail = ErrorResponseModel(code=status_code,
                                    router=request.url.path,
                                    params=dict(request.query_params),
                                    )
        raise HTTPException(status_code=status_code, detail=detail.dict())


# 从浏览器自动提取 Cookie
@router.post("/extract_browser_cookies",
             response_model=ResponseModel,
             summary="从浏览器自动提取Cookie/Extract cookies from browser")
async def extract_browser_cookies_api(request: Request,
                                       service: str = Body(default="douyin", description="服务名称/Service name")):
    """
    # [中文]
    ### 用途:
    - 从本地浏览器（Chrome/Firefox/Edge）自动提取对应平台的 Cookie 并加入池中。
    ### 参数:
    - service: 服务名称 (douyin / tiktok)
    ### 返回:
    - 提取结果及池状态

    # [English]
    ### Purpose:
    - Auto-extract cookies from local browsers (Chrome/Firefox/Edge) and add to pool.
    ### Parameters:
    - service: Service name (douyin / tiktok)
    ### Return:
    - Extraction result and pool status
    """
    try:
        if service not in ("douyin", "tiktok"):
            raise ValueError(f"不支持的服务 '{service}'。支持的服务: douyin, tiktok")

        pool = CookiePool.get_instance(service)
        count = await pool.auto_extract_from_browser()
        status = await pool.get_pool_status()

        return ResponseModel(
            code=200,
            router=request.url.path,
            data={
                "message": f"从浏览器提取到 {count} 个新 Cookie",
                "extracted": count,
                "pool_status": status,
            }
        )
    except Exception as e:
        status_code = 400
        detail = ErrorResponseModel(code=status_code,
                                    router=request.url.path,
                                    params=dict(request.query_params),
                                    )
        raise HTTPException(status_code=status_code, detail=detail.dict())

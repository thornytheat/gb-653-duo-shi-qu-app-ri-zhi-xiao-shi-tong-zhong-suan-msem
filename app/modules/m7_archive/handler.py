"""M7 冷热归档路由：页面 + JSON 接口。

接口清单
- GET  /archive                       归档工作台页面
- GET  /api/archive/status            待迁移/迁移中/已归档/失败与在线、冷占用
- GET  /api/archive/batches           批次清单（含四态）
- POST /api/archive/ingest            接入实际日志原件（热库，字节限额）
- POST /api/archive/migrate           按字节限额迁出旧批次（可恢复、幂等）
- POST /api/archive/readback          回读原件（热读热库，冷读逐帧校验冷件）
- POST /api/archive/verify            仅校验冷件完整性
- POST /api/archive/rebuild-buckets   回读原件并复算原桶
"""
from fastapi import APIRouter, Body, Request

from . import service

router = APIRouter(tags=["M7-冷热归档"])


@router.get("/archive")
def page(request: Request):
    store = service.get_store(request)
    data = {
        "status": service.status(store),
        "batches": service.batches(store)["batches"],
    }
    return request.app.state.templates.TemplateResponse(
        request, "m7_archive.html", {"title": "冷热归档", "data": data})


@router.get("/api/archive/status")
def get_status(request: Request):
    return service.status(service.get_store(request))


@router.get("/api/archive/batches")
def get_batches(request: Request):
    return service.batches(service.get_store(request))


@router.post("/api/archive/ingest")
def post_ingest(request: Request, payload: dict = Body(default={})):
    return service.ingest(service.get_store(request), payload)


@router.post("/api/archive/migrate")
def post_migrate(request: Request, payload: dict = Body(default={})):
    return service.migrate(service.get_store(request), payload)


@router.post("/api/archive/readback")
def post_readback(request: Request, payload: dict = Body(default={})):
    return service.readback(service.get_store(request), payload)


@router.post("/api/archive/verify")
def post_verify(request: Request, payload: dict = Body(default={})):
    return service.verify(service.get_store(request), payload)


@router.post("/api/archive/rebuild-buckets")
def post_rebuild(request: Request, payload: dict = Body(default={})):
    return service.rebuild(service.get_store(request), payload)

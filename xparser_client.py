
import asyncio
import httpx
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)
class XParserClient:
    """
    XParserClient - X-Parser 동기 API 클라이언트
    X-Parser의 단순한 동기 API를 비동기로 래핑하여
    병목 없이 여러 문서를 동시에 처리할 수 있습니다.
    """

    def __init__(self,
                 base_url: str,
                 timeout: int = 300,
                 max_concurrent: int = 10):
        self.base_url = base_url
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)  # 동시성 제한, I/O 바운드 작업이라 Semaphore 사용

    # TODO: 개인화 기능 활성화 시 bucket_name 기본값 변경 필요
    async def parse_document(self, saved_filename: str, bucket_name: str = "aidoc", options: Optional[Dict] = None) -> Dict[str, Any]:
        """
        문서 파싱 (X-Parser v1 API)

        Args:
            saved_filename: MinIO 객체 이름 (예: "documents/sample.pdf")
            bucket_name: MinIO 버킷 이름 (기본값: "aidoc")
            options: 파싱 옵션 (v1 API 스펙)

        Returns:
            파싱 결과 (name, origin, pages, content, enhanced_markdown)
        """
        async with self.semaphore:  # 동시성 제한
            request_data = {
                "saved_filename": saved_filename,
                "bucket_name": bucket_name,
                "options": self._build_options(options)
            }

            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(self.timeout)) as client:
                    logger.info(f"X-Parser 파싱 시작: {saved_filename}")
                    logger.debug(f"요청 옵션: {request_data['options']}")

                    response = await client.post(
                        f"{self.base_url}/api/v2/parse",
                        json=request_data
                    )
                    response.raise_for_status()

                    result = response.json()
                    logger.info(f"X-Parser 파싱 완료: {saved_filename}")
                    return result

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    raise ValueError(f"잘못된 요청: {e.response.text}")
                elif e.response.status_code == 404:
                    raise FileNotFoundError(f"파일 없음: {saved_filename}")
                elif e.response.status_code == 500:
                    error_text = e.response.text
                    raise RuntimeError(f"서버 오류: {error_text}")
                else:
                    logger.error(f"X-Parser HTTP 오류: {e.response.status_code} - {e.response.text}")
                    raise
            except Exception as e:
                logger.error(f"X-Parser 파싱 실패: {saved_filename} - {e}")
                raise
    async def parse_document_chunk(self, saved_filename: str, bucket_name: str = "aidoc", options: Optional[Dict] = None) -> Dict[str, Any]:
      
        async with self.semaphore:  # 동시성 제한
            request_data = {
                "saved_filename": saved_filename,
                "bucket_name": bucket_name,
                "options": self._build_options_v2(options)
            }

            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(self.timeout)) as client:
                    logger.info(f"X-Parser 파싱 시작: {saved_filename}")
                    logger.debug(f"요청 옵션: {request_data['options']}")

                    response = await client.post(
                        f"{self.base_url}/api/v2/chunk",
                        json=request_data
                    )
                    response.raise_for_status()

                    result = response.json()
                    logger.info(f"X-Parser 파싱 완료: {saved_filename}")
                    return result

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    raise ValueError(f"잘못된 요청: {e.response.text}")
                elif e.response.status_code == 404:
                    raise FileNotFoundError(f"파일 없음: {saved_filename}")
                elif e.response.status_code == 500:
                    error_text = e.response.text
                    raise RuntimeError(f"서버 오류: {error_text}")
                else:
                    logger.error(f"X-Parser HTTP 오류: {e.response.status_code} - {e.response.text}")
                    raise
            except Exception as e:
                logger.error(f"X-Parser 파싱 실패: {saved_filename} - {e}")
                raise


    async def _safe_parse_document(self, saved_filename: str, options: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
        """
        안전한 문서 파싱 (예외를 반환값으로 처리)

        배치 파싱에서 사용하며, 예외를 발생시키지 않고 None을 반환합니다.

        Args:
            saved_filename: MinIO 객체 이름
            options: 파싱 옵션

        Returns:
            파싱 결과 또는 None (실패 시)
        """
        try:
            return await self.parse_document(saved_filename, None, options)
        except Exception as e:
            logger.error(f"안전 파싱 실패: {saved_filename} - {e}")
            return None

    async def batch_parse_documents(self, saved_filenames: List[str], options: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        여러 문서 병렬 파싱

        Args:
            saved_filenames: 파싱할 MinIO 객체 이름 목록
            options: 공통 파싱 옵션

        Returns:
            파싱 결과 목록 (실패한 경우 None)
        """
        logger.info(f"배치 파싱 시작: {len(saved_filenames)}개 문서")

        tasks = [
            self._safe_parse_document(saved_filename, options)
            for saved_filename in saved_filenames
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 결과 정리
        processed_results = []
        success_count = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"배치 파싱 실패: {saved_filenames[i]} - {result}")
                processed_results.append(None)
            else:
                processed_results.append(result)
                success_count += 1

        logger.info(f"배치 파싱 완료: {success_count}/{len(saved_filenames)} 성공")
        return processed_results


    def _build_options(self, options: Optional[Dict] = None) -> Dict:
        """X-Parser v1 API 옵션 빌드"""
        default_options = {
            "response_format": "layout_json",      # v1에서는 고정
            "include_bbox": True,                  # 바운딩 박스 좌표 포함
            "id_marker": True,                     # 고유 ID 마커 추가
            "group_markers": True,                 # 그룹 마커 추가
            "use_xparser_description": True        # AI 설명 활성화
        }

        if options:
            default_options.update(options)

        return default_options
    
    def _build_options_v2(self, options: Optional[Dict] = None) -> Dict:
        """X-Parser v2 API 옵션 빌드"""
        default_options = {
            "use_ai_description": True,
            "table_format": "markdown"
        }

        if options:
            default_options.update(options)

        return default_options

    async def check_health(self) -> bool:
        """X-Parser 서버 상태 확인"""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{self.base_url}/health")

                if response.status_code == 200:
                    health_data = response.json()
                    return health_data.get("status") == "healthy"
                else:
                    return False

        except Exception as e:
            logger.warning(f"X-Parser 헬스체크 실패: {e}")
            return False

    async def get_server_info(self) -> Optional[Dict[str, Any]]:
        """서버 정보 조회"""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{self.base_url}/health")
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"서버 정보 조회 실패: {e}")
            return None

    def create_minimal_options(self) -> Dict[str, Any]:
        """최소 처리 옵션 (AI 비활성화)"""
        return {
            "use_xparser_description": False,
            "include_bbox": False,
            "group_markers": False
        }

    def create_full_options(self) -> Dict[str, Any]:
        """전체 기능 활성화 옵션"""
        return {
            "use_xparser_description": True,
            "include_bbox": True,
            "id_marker": True,
            "group_markers": True
        }


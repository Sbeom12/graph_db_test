from typing import Dict, Any, Tuple
from datetime import datetime
from xparser_client import XParserClient
from dotenv import load_dotenv
import os
import asyncio
import json
import logging

load_dotenv()
logger = logging.getLogger(__name__)


async def _parse_document(
    x_parser_client, request: Dict[str, Any]
) -> Tuple[Dict[str, Any], int]:
    """X-Parser를 사용하여 문서를 파싱합니다."""
    logger.info(f"Starting X-Parser parsing for: {request.get('file_path')}")
    parse_start = datetime.now()
    parse_result = await x_parser_client.parse_document_chunk(
        saved_filename=request["file_path"],
        bucket_name=request["bucket_name"],
        options=request.get("options", {}),
    )
    parse_time = int((datetime.now() - parse_start).total_seconds() * 1000)
    logger.info(f"X-Parser completed in {parse_time}ms")
    return parse_result, parse_time


async def main():
    xparser_url = os.getenv("X_PARSER_URL")
    xparser_client = XParserClient(base_url=xparser_url)
    file_names = [
        # "고객센터 - 비대면계좌개설.pdf",
        # "대신[Balance]ELB 156회 - 상품설명서.pdf",
        # "고객센터 - 업무시간 안내.pdf",
        # "기업분석 - [대신증권 이지은][4Q25 Preview] 엔씨소프트 아이온2 이후가 보이기 시작했다.pdf",
        # "고객센터 - 소비자보호조직도.pdf",
        "산업분석 - [대신증권 박혜진][Industry Report] 금융업 처음 보는 숫자, 75조원.pdf",
        # "고객센터 - 민원처리프로세스.pdf",
        # "고객센터 - 소비자보호기준.pdf",
        # "대신[Balance]CMA_개인_약관.pdf",
        # "모닝미팅 - [Morning Meeting Brief]_2026년 01월 27일.pdf",
        # "고객센터 - 수수료안내.pdf",
        # "경제전망 - [대신증권 리서치센터] 2026년 경제 및 금융시장 전망.pdf",
        # "삼성전자_분기보고서(2025.11.14).pdf",
    ]
    for file_name in file_names:
        request = {
            "bucket_name": "aidoc",
            "output_format": "layout_json",
            "file_path": file_name,
            "options": {"use_ai_description": True, "table_format": "markdown"},
        }
        print(f'start {file_name}')
        result = await _parse_document(xparser_client, request)
        print(f'finish {file_name}')
        with open(f"{file_name}.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4, ensure_ascii=False)
            
if __name__ == "__main__":
    asyncio.run(main())

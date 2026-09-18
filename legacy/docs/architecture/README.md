# 프로젝트 아키텍처 문서

이 문서는 `auto-invest` 저장소를 코드와 산출물 기준으로 파악한 결과다.

- 분석 기준 커밋: `66a2d78`
- 분석 기준일: 2026-09-09
- 정적 분석: Trailmark 0.5.0
- 문서 범위: 실시간 자동매매 앱, 리서치·백테스트 파이프라인, free-agent 투자위원회, GitHub Actions 자동화

## 문서 안내

- [project-overview.md](project-overview.md): 프로젝트가 무엇인지와 전체 경계
- [module-map.md](module-map.md): 디렉터리·모듈·주요 파일 지도
- [c4-context.md](c4-context.md): C4 시스템 컨텍스트
- [c4-containers.md](c4-containers.md): C4 컨테이너 구조
- [runtime-flows.md](runtime-flows.md): 실행·매매·리서치·Agent 흐름
- [data-model.md](data-model.md): SQLite 모델과 파일 기반 데이터 계보
- [code-graphs.md](code-graphs.md): Trailmark 호출 그래프와 복잡도 결과
- [unknowns-and-risks.md](unknowns-and-risks.md): 분석 한계와 확인이 필요한 위험


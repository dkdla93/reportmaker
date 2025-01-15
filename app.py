import streamlit as st
import pandas as pd
import numpy as np
import os
from io import BytesIO
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import zipfile
import jinja2
from datetime import datetime

def clean_numeric_value(value):
    """숫자 값을 안전하게 처리합니다."""
    try:
        if pd.isna(value):
            return 0.0
        if isinstance(value, str):
            # 쉼표와 공백 제거
            value = value.replace(',', '').strip()
            if value == '':
                return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def validate_input_data(revenue_df, song_df):
    """입력 데이터의 유효성을 검사합니다."""
    errors = []
    
    # 필수 컬럼 확인
    required_revenue_columns = ['앨범아티스트', '앨범명', '대분류', '중분류', '서비스명', '권리사정산금액']
    required_song_columns = ['아티스트명', '전월 잔액', '당월 차감액', '당월 잔액', '정산 요율']
    
    missing_revenue_cols = [col for col in required_revenue_columns if col not in revenue_df.columns]
    missing_song_cols = [col for col in required_song_columns if col not in song_df.columns]
    
    if missing_revenue_cols:
        errors.append(f"매출 정산 데이터 필수 컬럼 누락: {', '.join(missing_revenue_cols)}")
    if missing_song_cols:
        errors.append(f"곡비 정산 데이터 필수 컬럼 누락: {', '.join(missing_song_cols)}")
    
    # 데이터 존재 여부 확인
    if len(revenue_df) == 0:
        errors.append("매출 정산 데이터가 비어 있습니다.")
    if len(song_df) == 0:
        errors.append("곡비 정산 데이터가 비어 있습니다.")
    
    return errors

def process_data(revenue_data, song_data, artist):
    """아티스트별 정산 데이터를 처리합니다."""
    try:
        # 1. 아티스트 데이터 확인
        service_data = revenue_data[revenue_data['앨범아티스트'] == artist].copy()
        if len(service_data) == 0:
            raise ValueError(f"'{artist}'의 매출 데이터가 없습니다.")
        
        artist_song_data = song_data[song_data['아티스트명'] == artist]
        if len(artist_song_data) == 0:
            raise ValueError(f"'{artist}'의 곡비 데이터가 없습니다.")
        
        # 2. 정렬 순서 정의
        sort_order = {
            '대분류': ['국내', '해외', 'YouTube'],
            '중분류': ['광고수익', '구독수익', '기타', '스트리밍'],
            '서비스명': ['기타 서비스', '스트리밍', '스트리밍 (음원)', 'Art Track', 'Sound Recording']
        }

        # 3. 음원 서비스별 정산내역 데이터 생성
        service_summary = service_data.groupby(
            ['앨범명', '대분류', '중분류', '서비스명']
        )['매출 순수익'].sum().reset_index()

        # 정렬을 위한 임시 컬럼 생성
        for col in ['대분류', '중분류', '서비스명']:
            service_summary[f'{col}_sort'] = service_summary[col].map(
                {v: i for i, v in enumerate(sort_order[col])}
            ).fillna(len(sort_order[col]))

        # 정렬 적용
        service_summary = service_summary.sort_values(
            by=['앨범명', '대분류_sort', '중분류_sort', '서비스명_sort']
        ).drop(['대분류_sort', '중분류_sort', '서비스명_sort'], axis=1)

        # 4. 앨범별 정산내역 데이터 생성
        album_summary = service_data.groupby(['앨범명'])['매출 순수익'].sum().reset_index()
        album_summary = album_summary.sort_values('앨범명')
        total_revenue = float(album_summary['매출 순수익'].sum())

        # 5. 공제 내역 데이터 생성
        artist_song_row = artist_song_data.iloc[0]
        
        # 디버깅: 곡비 데이터 출력
        st.write(f"### {artist}의 곡비 데이터:")
        st.write({
            '전월 잔액': artist_song_row['전월 잔액'],
            '당월 차감액': artist_song_row['당월 차감액'],
            '당월 잔액': artist_song_row['당월 잔액'],
            '정산 요율': artist_song_row['정산 요율']
        })
        
        # 데이터 타입 확인 및 변환
        previous_balance = clean_numeric_value(artist_song_row['전월 잔액'])
        current_deduction = clean_numeric_value(artist_song_row['당월 차감액'])
        current_balance = clean_numeric_value(artist_song_row['당월 잔액'])
        revenue_share_rate = clean_numeric_value(artist_song_row['정산 요율'])
        
        # 디버깅: 변환된 데이터 출력
        st.write("### 변환된 데이터:")
        st.write({
            '전월 잔액(변환)': previous_balance,
            '당월 차감액(변환)': current_deduction,
            '당월 잔액(변환)': current_balance,
            '정산 요율(변환)': revenue_share_rate
        })
        
        deduction_data = {
            '곡비': previous_balance,
            '공제 금액': current_deduction,
            '공제 후 남은 곡비': current_balance,
            '공제 적용 금액': float(total_revenue - current_deduction)
        }

        # 6. 수익 배분 데이터 생성
        applied_amount = float(deduction_data['공제 적용 금액'] * revenue_share_rate)
        distribution_data = {
            '항목': '수익 배분율',
            '적용율': revenue_share_rate,
            '적용 금액': applied_amount
        }
        
        # 디버깅: 최종 데이터 출력
        st.write("### 최종 계산 결과:")
        st.write({
            '총 매출': total_revenue,
            '공제 내역': deduction_data,
            '수익 배분': distribution_data
        })

        return service_summary, album_summary, total_revenue, deduction_data, distribution_data
    except Exception as e:
        st.error(f"데이터 처리 중 오류 발생 ({artist}): {str(e)}")
        st.write("### 오류 발생 시점의 데이터:")
        try:
            st.write({
                '아티스트': artist,
                '곡비 데이터 존재': len(artist_song_data) > 0,
                '매출 데이터 존재': len(service_data) > 0,
                '총 매출': total_revenue if 'total_revenue' in locals() else 'N/A'
            })
        except:
            pass
        raise

def create_html_content(artist, issue_date, service_summary, album_summary, total_revenue, deduction_data, distribution_data):
    """HTML 보고서를 생성합니다."""
    template = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8">
        <style>
            body { 
                font-family: Arial, sans-serif; 
                padding: 40px;
                max-width: 1000px;
                margin: 0 auto;
                background-color: #ffffff;
                line-height: 1.6;
            }
            .issue-date {
                text-align: right;
                margin-bottom: 20px;
                font-size: 14px;
                color: #333;
            }
            .report-period {
                font-size: 22px;
                font-weight: bold;
                margin-bottom: 15px;
                color: #333;
            }
            .report-title {
                background-color: #e8f4f8;
                padding: 15px;
                text-align: center;
                border-radius: 8px;
                margin-bottom: 25px;
                font-size: 20px;
                font-weight: bold;
                color: #2c3e50;
            }
            .info-section {
                margin-bottom: 25px;
                line-height: 1.8;
            }
            .info-text {
                margin-bottom: 6px;
                color: #555;
            }
            .email-info {
                margin-bottom: 30px;
                color: #2980b9;
            }
            .section-title {
                font-size: 16px;
                font-weight: bold;
                margin: 30px 0 15px 0;
                color: #2c3e50;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 25px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            }
            th {
                background-color: #8cc4de;
                color: #2c3e50;
                padding: 12px 8px;
                text-align: center;
                border: 1px solid #a5d1e4;
                font-size: 14px;
            }
            td {
                padding: 10px 8px;
                border: 1px solid #e1ecf2;
                font-size: 14px;
            }
            tr:nth-child(even) {
                background-color: #f8fbfd;
            }
            .number-cell {
                text-align: right;
                font-family: 'Courier New', monospace;
                font-weight: bold;
            }
            .center-cell {
                text-align: center;
            }
            .total-row {
                background-color: #f5f9fb !important;
                font-weight: bold;
            }
            .total-row td {
                border-top: 2px solid #8cc4de;
            }
            .note {
                font-size: 13px;
                text-align: right;
                margin-top: 20px;
                color: #666;
            }
            .gray-bg {
                background-color: #f8f9fa;
            }
            .unprocessed-artists {
                background-color: #fff3cd;
                border: 1px solid #ffeeba;
                padding: 15px;
                margin-bottom: 20px;
                border-radius: 5px;
            }
        </style>
    </head>
    <body>
        <div class="issue-date">{{ issue_date }}</div>
        <div class="report-period">2024년 12월 판매분</div>
        <div class="report-title">{{ artist }}님 음원 정산 내역서</div>
        
        <div class="info-section">
            <div class="info-text">* 저희와 함께해 주셔서 정말 감사하고 앞으로도 잘 부탁드리겠습니다!</div>
            <div class="info-text">* 2024년 12월 음원의 수익을 아래와 같이 정산드리오니 참고 부탁드립니다.</div>
            <div class="info-text">* 정산 관련하여 문의사항이 있다면 무엇이든, 언제든 편히 메일 주세요!</div>
            <div class="email-info">E-Mail : lucasdh3013@naver.com</div>
        </div>
        
        <div class="section-title">1. 음원 서비스별 정산내역</div>
        <table>
            <thead>
                <tr>
                    <th>앨범명</th>
                    <th>대분류</th>
                    <th>중분류</th>
                    <th>서비스명</th>
                    <th>기간</th>
                    <th>매출액</th>
                </tr>
            </thead>
            <tbody>
                {% for _, row in service_summary.iterrows() %}
                <tr>
                    <td>{{ row['앨범명'] }}</td>
                    <td>{{ row['대분류'] }}</td>
                    <td>{{ row['중분류'] }}</td>
                    <td>{{ row['서비스명'] }}</td>
                    <td class="center-cell">2024년 12월</td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(row['매출 순수익']) }}</td>
                </tr>
                {% endfor %}
                <tr class="total-row">
                    <td colspan="5">합계</td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(total_revenue) }}</td>
                </tr>
            </tbody>
        </table>
        
        <div class="section-title">2. 앨범별 정산내역</div>
        <table>
            <thead>
                <tr>
                    <th>앨범명</th>
                    <th>기간</th>
                    <th>매출액</th>
                </tr>
            </thead>
            <tbody>
                {% for _, row in album_summary.iterrows() %}
                <tr>
                    <td>{{ row['앨범명'] }}</td>
                    <td class="center-cell">2024년 12월</td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(row['매출 순수익']) }}</td>
                </tr>
                {% endfor %}
                <tr class="total-row">
                    <td colspan="2">합계</td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(total_revenue) }}</td>
                </tr>
            </tbody>
        </table>
        
        <div class="section-title">3. 공제 내역</div>
        <table class="gray-bg">
            <thead>
                <tr>
                    <th>앨범</th>
                    <th>곡비</th>
                    <th>공제 금액</th>
                    <th>공제 후 남은 곡비</th>
                    <th>공제 적용 금액</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td></td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(deduction_data['곡비']) }}</td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(deduction_data['공제 금액']) }}</td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(deduction_data['공제 후 남은 곡비']) }}</td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(deduction_data['공제 적용 금액']) }}</td>
                </tr>
            </tbody>
        </table>
        
        <div class="section-title">4. 수익 배분</div>
        <table class="gray-bg">
            <thead>
                <tr>
                    <th>앨범</th>
                    <th>항목</th>
                    <th>적용율</th>
                    <th></th>
                    <th>적용 금액</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td></td>
                    <td>{{ distribution_data['항목'] }}</td>
                    <td class="center-cell">{{ "{:.1%}".format(distribution_data['적용율']) }}</td>
                    <td></td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(distribution_data['적용 금액']) }}</td>
                </tr>
                <tr class="total-row">
                    <td colspan="4" class="total-label">총 정산금액</td>
                    <td class="number-cell">₩{{ "{:,.0f}".format(distribution_data['적용 금액']) }}</td>
                </tr>
            </tbody>
        </table>
        
        <div class="note">* 부가세 별도</div>
    </body>
</html>
    """
    
    # Jinja2 템플릿 렌더링
    template = jinja2.Template(template)
    html_content = template.render(
        artist=artist,
        issue_date=issue_date,
        service_summary=service_summary,
        album_summary=album_summary,
        total_revenue=total_revenue,
        deduction_data=deduction_data
        )

def generate_reports(revenue_file, song_file, issue_date):
    """보고서를 생성하고 ZIP 파일로 압축합니다."""
    try:
        # 1. 엑셀 파일 읽기
        try:
            revenue_df = pd.read_excel(revenue_file)
            song_df = pd.read_excel(song_file)
        except Exception as e:
            raise ValueError(f"엑셀 파일 읽기 실패: {str(e)}")
        
        # 2. 입력 데이터 검증
        validation_errors = validate_input_data(revenue_df, song_df)
        if validation_errors:
            raise ValueError("\n".join(validation_errors))
        
        # 3. 매출 순수익으로 컬럼명 변경
        if '매출 순수익' not in revenue_df.columns and '권리사정산금액' in revenue_df.columns:
            revenue_df = revenue_df.rename(columns={'권리사정산금액': '매출 순수익'})
        
        # 4. 숫자 데이터 전처리
        revenue_df['매출 순수익'] = revenue_df['매출 순수익'].apply(clean_numeric_value)
        numeric_columns = ['전월 잔액', '당월 차감액', '당월 잔액', '정산 요율']
        for col in numeric_columns:
            song_df[col] = song_df[col].apply(clean_numeric_value)
        
        # 5. 아티스트 목록 추출
        artists = revenue_df['앨범아티스트'].unique()
        if len(artists) == 0:
            raise ValueError("아티스트 정보를 찾을 수 없습니다.")
        
        # 6. 처리 상태 추적
        processed_artists = []
        failed_artists = []
        
        # 7. ZIP 파일 생성
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            progress_bar = st.progress(0)
            
            for idx, artist in enumerate(artists, 1):
                try:
                    # 데이터 처리
                    service_summary, album_summary, total_revenue, deduction_data, distribution_data = process_data(
                        revenue_df, song_df, artist
                    )
                    
                    # HTML 보고서 생성
                    html_content = create_html_content(
                        artist=artist,
                        issue_date=issue_date,
                        service_summary=service_summary,
                        album_summary=album_summary,
                        total_revenue=total_revenue,
                        deduction_data=deduction_data,
                        distribution_data=distribution_data
                    )
                    
                    if html_content:
                        # HTML 파일 저장
                        html_file_name = f"정산서_{artist}_202412.html"
                        zip_file.writestr(f"html/{html_file_name}", html_content.encode('utf-8'))
                        
                        # 세부매출내역 엑셀 파일 생성
                        excel_buffer = BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            service_summary.to_excel(writer, index=False, sheet_name='세부매출내역')
                        
                        excel_buffer.seek(0)
                        excel_file_name = f"세부매출내역_{artist}_202412.xlsx"
                        zip_file.writestr(f"excel/{excel_file_name}", excel_buffer.getvalue())
                        
                        processed_artists.append(artist)
                    
                except Exception as e:
                    failed_artists.append((artist, str(e)))
                    st.error(f"{artist} 처리 중 오류 발생: {str(e)}")
                    continue
                
                finally:
                    progress_bar.progress(idx / len(artists))
            
            # 8. 처리 결과 로그 생성
            log_content = f"""처리 결과 요약
============================
총 아티스트 수: {len(artists)}
처리 성공: {len(processed_artists)}
처리 실패: {len(failed_artists)}

실패한 아티스트 목록:
{chr(10).join([f"- {artist}: {error}" for artist, error in failed_artists])}
"""
            zip_file.writestr('processing_log.txt', log_content)
            
            progress_bar.empty()
        
        # 9. 결과 반환
        zip_buffer.seek(0)
        verification_result = {
            'total_artists': len(artists),
            'processed_artists': len(processed_artists),
            'failed_artists': failed_artists,
            'unprocessed_artists': [artist for artist in artists if artist not in processed_artists]
        }
        
        return zip_buffer, len(processed_artists), verification_result
        
    except Exception as e:
        st.error(f"보고서 생성 중 오류 발생: {str(e)}")
        return None, 0, None

def main():
    try:
        st.title("아티스트별 정산서 생성 프로그램")
        
        st.write("📊 정산 데이터 파일들을 업로드하면 아티스트별 정산서가 자동으로 생성됩니다.")
        
        # 발행일자 입력
        issue_date = st.date_input(
            "정산서 발행일자를 선택하세요",
            value=pd.Timestamp('2025-01-15'),
            format="YYYY-MM-DD"
        ).strftime('%Y. %m. %d')
        
        # 파일 업로드
        revenue_file = st.file_uploader(
            "매출 정산 데이터 파일을 업로드하세요", 
            type=['xlsx'], 
            key="revenue",
            help="매출 정산 데이터가 포함된 Excel 파일을 선택하세요."
        )
        
        song_file = st.file_uploader(
            "곡비 정산 데이터 파일을 업로드하세요", 
            type=['xlsx'], 
            key="song",
            help="곡비 정산 데이터가 포함된 Excel 파일을 선택하세요."
        )
        
        if revenue_file is not None and song_file is not None:
            if st.button("보고서 생성", help="클릭하면 정산서 생성이 시작됩니다."):
                with st.spinner('보고서 생성 중...'):
                    zip_buffer, processed_count, verification_result = generate_reports(
                        revenue_file, song_file, issue_date
                    )
                    
                    if zip_buffer and verification_result:
                        st.success(f"총 {verification_result['total_artists']}명 중 "
                                 f"{processed_count}명의 아티스트 정산서가 생성되었습니다!")
                        
                        # 처리되지 않은 아티스트 표시
                        if verification_result['unprocessed_artists']:
                            with st.expander("⚠️ 처리되지 않은 아티스트 목록", expanded=True):
                                st.warning("다음 아티스트들의 정산서가 생성되지 않았습니다:")
                                for artist in verification_result['unprocessed_artists']:
                                    st.write(f"- {artist}")
                        
                        # 다운로드 버튼
                        st.download_button(
                            label="📥 전체 정산서 다운로드 (ZIP)",
                            data=zip_buffer,
                            file_name=f"정산서_전체_202412.zip",
                            mime="application/zip",
                            help="생성된 모든 정산서를 ZIP 파일로 다운로드합니다."
                        )
    except Exception as e:
        st.error(f"프로그램 실행 중 오류가 발생했습니다: {str(e)}")

if __name__ == "__main__":
    main()

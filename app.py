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
            return 0
        if isinstance(value, str):
            value = value.replace(',', '')
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def verify_artist_processing(revenue_df, processed_artists):
    """모든 아티스트가 처리되었는지 검증합니다."""
    # 매출 정산 데이터에서 추출된 모든 아티스트
    all_artists_in_revenue = set(revenue_df['앨범아티스트'].unique())
    
    # 처리된 아티스트
    processed_artists_set = set(processed_artists)
    
    # 처리되지 않은 아티스트 확인
    unprocessed_artists = all_artists_in_revenue - processed_artists_set
    
    return {
        'total_artists': len(all_artists_in_revenue),
        'processed_artists': len(processed_artists_set),
        'unprocessed_artists': list(unprocessed_artists),
        'all_processed': len(unprocessed_artists) == 0
    }

def process_data(revenue_data, song_data, artist):
    """아티스트별 정산 데이터를 처리합니다."""
    # 정렬 순서 정의
    sort_order = {
        '대분류': ['국내', '해외', 'YouTube'],
        '중분류': ['광고수익', '구독수익', '기타', '스트리밍'],
        '서비스명': ['기타 서비스', '스트리밍', '스트리밍 (음원)', 'Art Track', 'Sound Recording']
    }

    # 1. 음원 서비스별 정산내역 데이터 생성
    service_data = revenue_data[revenue_data['앨범아티스트'] == artist].copy()
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

    # 2. 앨범별 정산내역 데이터 생성
    album_summary = service_data.groupby(['앨범명'])['매출 순수익'].sum().reset_index()
    album_summary = album_summary.sort_values('앨범명')
    total_revenue = float(album_summary['매출 순수익'].sum())

    # 3. 공제 내역 데이터 생성
    # 아티스트 정보가 없으면 예외 처리
    artist_song_data = song_data[song_data['아티스트명'] == artist]
    if artist_song_data.empty:
        raise ValueError(f"아티스트 '{artist}'에 대한 곡비 정보를 찾을 수 없습니다.")
    
    artist_song_data = artist_song_data.iloc[0]
    deduction_data = {
        '곡비': float(artist_song_data['전월 잔액']),
        '공제 금액': float(artist_song_data['당월 차감액']),
        '공제 후 남은 곡비': float(artist_song_data['당월 잔액']),
        '공제 적용 금액': float(total_revenue - artist_song_data['당월 차감액'])
    }

    # 4. 수익 배분 데이터 생성
    distribution_data = {
        '항목': '수익 배분율',
        '적용율': float(artist_song_data['정산 요율']),
        '적용 금액': float(deduction_data['공제 적용 금액'] * artist_song_data['정산 요율'])
    }

    return service_summary, album_summary, total_revenue, deduction_data, distribution_data

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
        # 엑셀 파일 읽기
        revenue_df = pd.read_excel(revenue_file)
        song_df = pd.read_excel(song_file)
        
        # 매출 순수익으로 컬럼명 변경
        revenue_df = revenue_df.rename(columns={'권리사정산금액': '매출 순수익'})
        
        # 아티스트 목록 추출
        artists = revenue_df['앨범아티스트'].unique()
        if len(artists) == 0:
            raise ValueError("아티스트 정보를 찾을 수 없습니다.")
        
        # ZIP 파일을 저장할 메모리 버퍼
        zip_buffer = BytesIO()
        
        # ZIP 파일 생성
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for artist in artists:
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
                        
                except Exception as e:
                    st.error(f"{artist} 처리 중 오류 발생: {str(e)}")
                    continue
        
        zip_buffer.seek(0)
        return zip_buffer, len(artists)
        
    except Exception as e:
        st.error(f"보고서 생성 중 오류 발생: {str(e)}")
        return None, 0

def main():
    st.title("아티스트별 정산서 생성 프로그램")
    
    st.write("📊 정산 데이터 파일들을 업로드하면 아티스트별 정산서가 자동으로 생성됩니다.")
    
    # 발행일자 입력 (포맷 수정)
    issue_date = st.date_input(
        "정산서 발행일자를 선택하세요",
        value=pd.Timestamp('2025-01-15'),
        format="YYYY-MM-DD"  # 형식 변경
    ).strftime('%Y. %m. %d')  # 표시 형식은 원하는 대로 변환
    
    # 파일 업로드
    revenue_file = st.file_uploader("매출 정산 데이터 파일을 업로드하세요", type=['xlsx'], key="revenue")
    song_file = st.file_uploader("곡비 정산 데이터 파일을 업로드하세요", type=['xlsx'], key="song")
    
    if revenue_file is not None and song_file is not None:
        if st.button("보고서 생성"):
            with st.spinner('보고서 생성 중...'):
                # 변경된 부분: generate_reports()의 반환값이 3개로 변경
                zip_buffer, artist_count, verification_result = generate_reports(revenue_file, song_file, issue_date)
                
                if zip_buffer and artist_count > 0:
                    # 새로 추가된 성공 메시지
                    st.success(f"총 {verification_result['total_artists']}명 중 {artist_count}명의 아티스트 정산서가 생성되었습니다!")
                    
                    # 처리되지 않은 아티스트가 있다면 경고 표시
                    if not verification_result['all_processed']:
                        st.warning("일부 아티스트가 처리되지 않았습니다:")
                        for artist in verification_result['unprocessed_artists']:
                            st.warning(f"- {artist}")
                    
                    # 다운로드 버튼
                    st.download_button(
                        label="📥 전체 정산서 다운로드 (ZIP)",
                        data=zip_buffer,
                        file_name=f"정산서_전체_202412.zip",
                        mime="application/zip"
                    )

if __name__ == "__main__":
    main()

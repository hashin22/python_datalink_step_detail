import pandas as pd
import tkinter as tk
import os
import pprint
from tkinter import filedialog
from collections import defaultdict
from tqdm.auto import tqdm



# 파일 가져오기

SHEET = print('전체상담내역 파일 선택: ')
print("전체상담내역 파일 선택: ", end=' ')
root = tk.Tk()
root.withdraw()
call_path = filedialog.askopenfile(title="전체상담내역 파일 선택", filetypes=(("Text Files", "*.xlsx"),)).name
print(call_path)
SHEET = print('고객입력정보 변환 파일 선택: ')
print("고객입력정보 변환 파일 선택: ", end=' ')
stt_path = filedialog.askopenfile(title="고객입력정보 변환 파일 선택", filetypes=(("Text Files", "*.xlsx"),)).name
print(stt_path)
# --------------------------------------------------------
# 데이터 로드 및 전처리
# --------------------------------------------------------
df_stt = pd.read_excel(stt_path, dtype={'세션아이디': str})
df_call = pd.read_excel(call_path, dtype={'세션 아이디': str})

df_stt.rename(columns={"세션아이디": "session_id", "전화번호": "phone_number"}, inplace=True)
df_call.rename(columns={"세션 아이디": "session_id", "전화번호": "phone_number"}, inplace=True)

# JOIN - OUTER
df = df_stt.merge(
    df_call,
    how='outer',
    left_on=['session_id', 'phone_number', '날짜'],
    right_on=['session_id', 'phone_number', '날짜']
)

# 존재 안 할 수도 있는 컬럼 드랍은 에러 방지용
for col in ['이상키워드', '시간']:
    if col in df.columns:
        df.drop(columns=[col], inplace=True)

df.sort_values(['날짜', '시작시간', 'session_id'], inplace=True, ignore_index=True)
df = df[['session_id', '날짜', '시작시간', '통화결과', '마지막 대화', '대화명', 'STT']]
df.drop_duplicates(inplace=True)
print(f'{" JOIN ":=^50}')

# Null Check
df[['대화명', 'STT']] = df[['대화명', 'STT']].fillna('N')
print(f'{" NULL ":=^50}')
pprint.pprint(df.isna().sum())

# ---------------------------------------------------------
# STEP (대화명 TTS)
# ---------------------------------------------------------
print(f'{" ADD COLUMN(STEP) ":=^50}')
df['STEP'] = df['대화명']
df['STEP'] = df.STEP.str.replace(r"( |_)(N|Y|모호)", "", regex=True)
df['STEP'] = df.STEP.str.replace(r" .+(_.+)?", "", regex=True)

# 대화명 2회 찍히는 단계 삭제
filter1 = df.session_id.isin(df.loc[df['대화명'] == '증상_구독분기', 'session_id'])
df.drop(df[(filter1) & (df['대화명'] == '렌탈')].index, inplace=True)
filter2 = df.session_id.isin(df.loc[df['대화명'] == '질의 1 모호', 'session_id'])
df.drop(df[(filter2) & (df['대화명'] == '질의 1 N')].index, inplace=True)
filter3 = df.session_id.isin(df.loc[df['대화명'] == '질의 2 모호', 'session_id'])
df.drop(df[(filter3) & (df['대화명'] == '질의 2 N')].index, inplace=True)
# ---------------------------------------------------------
# FLOWS (세션별 단계 집합 만들기)
# ---------------------------------------------------------
print(f'{" FLOWS ":=^50}')
flows = []
steps = set()
# ★ FIX: step_set을 hashable 타입으로 초기화 (set/dict → frozenset)
df['step_set'] = [frozenset() for _ in range(df.shape[0])]  # ★ FIX
df.reset_index(drop=True, inplace=True)

for row in tqdm(df[:-1].iterrows(), total=df.shape[0] - 1):
    i = row[0]
    r = row[1]
    if r['STEP'] != '상담사 연결 요청':
        steps.add(r['STEP'])

    # 세션 경계
    if r['session_id'] != df.loc[i + 1, 'session_id']:
        if steps not in flows:
            flows.append(steps)
        # ★ FIX: 세션 내 모든 행의 step_set을 frozenset으로 채움

        step_fs = frozenset(steps)  # ★ FIX

        cnt = (df.session_id == r['session_id']).sum()  # ★ FIX

        df.loc[df.session_id == r['session_id'], 'step_set'] = [step_fs] * cnt  # ★ FIX

        steps = set()


# 빈 set 제거 안전 처리
try:

    flows.remove(set())  # ★ FIX (기존: flows.pop(flows.index(set())))

except ValueError:

    pass



# ---------------------------------------------------------
# TURN 추가
# ---------------------------------------------------------

print(f'{" TURN ":=^50}')
df['turn'] = 0
flows = sorted(flows, key=lambda x: len(x), reverse=True)
df.sort_values(['날짜', '시작시간', 'session_id'], inplace=True, ignore_index=True)


for steps in tqdm(flows):
    steps_subset = sorted([flow for flow in flows if flow.issubset(steps)], key=lambda x: len(x))  # 부분집합들



    # turn2set
    turn2set = defaultdict(list)
    for ss in steps_subset:
        turn2set[len(ss)].append(ss)
    turn2set[0] = [{}]


    # 중복단계 체크
    turns = sorted(turn2set.keys(), reverse=True)
    for t in turns:
        tsets = turn2set[t]
        if len(tsets) > 1:
            if len(turn2set[t + 1]) > 0:
                i = 0
                while i < len(tsets):
                    for uss in turn2set[t + 1]:
                        if tsets[i].issubset(uss):
                            i += 1
                            break
                    else:  # t+1 단계의 부분집합이 아니면 삭제

                        tsets.pop(i)

        if len(turn2set[t]) > 1:
            if len(turn2set[t - 1]) > 1:
                i = 0
                while i < len(turn2set[t]):
                    if all([dss.issubset(turn2set[t][i]) for dss in turn2set[t - 1]]):
                        i += 1
                    else:  # t-1 단계를 다 포함하지 않으면 삭제
                        turn2set[t].pop(i)



    # steps_subset 업데이트
    steps_subset = []
    for t in turns[:-1]:
        steps_subset += turn2set[t]
    steps_subset



    # ★ FIX: isin 비교를 위해 frozenset 리스트로 변환
    steps_subset_fs = [frozenset(ss) for ss in steps_subset]  # ★ FIX

    # turn, step{t} 입력
    turn2set_dict = dict(turn2set)
    for t, sss in turn2set_dict.items():
        if t == 0:
            continue
        for ss in sss:
            tb = t - 1
            while len(turn2set[tb]) == 0:
                tb -= 1
            for s in ss.difference(turn2set[tb][0]):
                if s == '':
                    print(s, t)
                df.loc[
                    (df['turn'] == 0) &
                    (df['STEP'] == s) &
                    (df['step_set'].isin(steps_subset_fs)),  # ★ FIX
                    'turn'
                ] = t
df.drop(columns=['step_set'], inplace=True)
# TURN 예외
df.loc[(df['마지막 대화'] == "세척서비스 Q") & (df['대화명'].str.contains('제품')), 'turn'] = 2
# ---------------------------------------------------------
# LAST 추가
# ---------------------------------------------------------
df.sort_values(['날짜', '시작시간', 'session_id', 'turn'], inplace=True, ignore_index=True)
df['LAST'] = "N"
df.loc[((df.session_id.shift(-1) != df.session_id)), "LAST"] = "Y"
# ---------------------------------------------------------
# 누적STEP (STT, TTS) 추가
# ---------------------------------------------------------
print(f'{" ACC STEP ":=^50}')
acc_stt_list = []
acc_tts_list = []
acc_stt_value = ""
acc_tts_value = ""



for i, v in df[['대화명', 'STEP', 'LAST', 'turn']].iterrows():
    if v['turn'] == 1:
        acc_stt_value += f"{v['대화명']}"
        acc_tts_value += f"{v['STEP']}"
    elif v['turn'] > 1:
        acc_stt_value += f"->{v['대화명']}"
        acc_tts_value += f"->{v['STEP']}"
    acc_stt_list.append(acc_stt_value)
    acc_tts_list.append(acc_tts_value)
    if v['LAST'] == "Y":
        acc_stt_value = ""
        acc_tts_value = ""


df['acc_stt_step'] = acc_stt_list
df['acc_tts_step'] = acc_tts_list



# Call STT STEPS
call_stt_step = df.loc[df['LAST'] == 'Y', ['session_id', 'acc_stt_step']].drop_duplicates(subset='session_id')
call_stt_step.rename(columns={'acc_stt_step': 'call_stt_step'}, inplace=True)
df = df.merge(call_stt_step, how='left', on='session_id')


# Call TTS STEPS
call_tts_step = df.loc[df['LAST'] == 'Y', ['session_id', 'acc_tts_step']].drop_duplicates(subset='session_id')
call_tts_step.rename(columns={'acc_tts_step': 'call_tts_step'}, inplace=True)
df = df.merge(call_tts_step, how='left', on='session_id')



# 콜 넘버링하기
print(f'{" Call Number ":=^50}')
df.sort_values(['날짜', '시작시간', 'session_id', 'turn'], inplace=True, ignore_index=True)
nums = []
sid_b = 0
n = 0
for sid in df.session_id:
    if sid != sid_b:
        n += 1
    nums.append(n)
    sid_b = sid
df['No'] = nums


# No, 발화턴 앞으로 재배치
front_cols = ['No', 'turn']
other_cols = [col for col in df.columns if col not in front_cols]
df = df[front_cols + other_cols]

# 제품
print(f'{" PRODUCT ":=^50}')
# 에어컨
df.loc[df.session_id.isin(df.loc[df['대화명'].str.contains('에어컨'), 'session_id']), ['PRODUCT_GROUP', '제품']] = ["에어컨/에어케어", "에어컨"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (에어컨)_벽걸이형', 'session_id']), 'PRODUCT_CODE'] = "WASRA"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (에어컨)_스탠드형', 'session_id']), 'PRODUCT_CODE'] = "STPAC"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (에어컨)_천장형', 'session_id']), 'PRODUCT_CODE'] = "RECRB"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (에어컨)_모호', 'session_id']), 'PRODUCT_CODE'] = "TWPAT"
df.loc[df.session_id.isin(df.loc[(df['대화명']=='세부 제품 확인 (에어컨)_그외_11009')&(df.STT.str.contains('이동')), 'session_id']), 'PRODUCT_CODE'] = "REWRA"
df.loc[df.session_id.isin(df.loc[(df['대화명']=='세부 제품 확인 (에어컨)_그외_11009')&(df.STT.str.contains('창')), 'session_id']), 'PRODUCT_CODE'] = "WIWRA"
df.loc[df.session_id.isin(df.loc[(df['대화명']=='세부 제품 확인 (에어컨)_그외_11009')&(df.STT.str.contains('투')), 'session_id']), 'PRODUCT_CODE'] = "TWPAT"
df.loc[df.session_id.isin(df.loc[(df['대화명']=='세부 제품 확인 (에어컨)_그외_11009')&(df['STT'].str.contains('트')), 'session_id']), 'PRODUCT_CODE'] = "TWPAT"
df.loc[df.session_id.isin(df.loc[(df['대화명']=='세부 제품 확인 (에어컨)_그외_11069')&(df['STT'].str.contains('스탠')), 'session_id']), 'PRODUCT_CODE'] = "COPAH"
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11009)')&(df['STT'].str.contains('벽')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["에어컨/에어케어", "에어컨", "WASRA"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11009)')&(df['STT'].str.contains('창')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["에어컨/에어케어", "에어컨", "WIWRA"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11069)')&(df['STT'].str.contains('시스템|천장|천정')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["에어컨/에어케어", "에어컨", "RECRB"]

# 냉장고
df.loc[df.session_id.isin(df.loc[df['대화명'].str.contains('냉장고'), 'session_id']), ['PRODUCT_GROUP', '제품']] = ["주방가전", "냉장고"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (냉장고)_양문형', 'session_id']), 'PRODUCT_CODE'] = "SISBS"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (냉장고)_모호', 'session_id']), 'PRODUCT_CODE'] = "SISBS"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (냉장고)_일반형', 'session_id']), 'PRODUCT_CODE'] = "NOREF"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (냉장고)_정수기형', 'session_id']), 'PRODUCT_CODE'] = "PUSBS"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (냉장고)_포도어', 'session_id']), 'PRODUCT_CODE'] = "ULSBS"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (김치냉장고)_부정', 'session_id']), 'PRODUCT_CODE'] = "STKRE"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (김치냉장고)_긍정', 'session_id']), 'PRODUCT_CODE'] = "COKRE"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (냉장고)_비즈니스', 'session_id']), 'PRODUCT_CODE'] = "BUSBS"
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11006)')&(df.STT.str.contains(r'(업소용|영업용)')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["주방가전", "냉장고", "BUSBS"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11006)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (김치냉장고)_부정'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["주방가전", "냉장고", "STKRE"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11006)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (김치냉장고)_긍정'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["주방가전", "냉장고", "COKRE"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11006)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (냉장고)_일반형'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["주방가전", "냉장고", "NOREF"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11006)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (냉장고)_정수기형'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["주방가전", "냉장고", "PUSBS"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11006)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (냉장고)_양문형'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["주방가전", "냉장고", "SISBS"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11006)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (냉장고)_포도어'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["주방가전", "냉장고", "ULSBS"]

# 세탁기 
df.loc[df.session_id.isin(df.loc[df['대화명']=='제품 대분류 (세탁기)', 'session_id']), ['PRODUCT_GROUP', '제품']] = ["생활가전", "세탁기"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (세탁기)_워시타워', 'session_id']), 'PRODUCT_CODE'] = "WADWS"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (세탁기)_드럼', 'session_id']), 'PRODUCT_CODE'] = "DUDRW"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (세탁기)_통돌이', 'session_id']), 'PRODUCT_CODE'] = "GEDRW"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (세탁기)_워시콤보', 'session_id']), 'PRODUCT_CODE'] = "WACOMBO"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (세탁기)_의류건조기', 'session_id']), 'PRODUCT_CODE'] = "CLDRR"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (세탁기)_미니세탁기', 'session_id']), 'PRODUCT_CODE'] = "MIDRW"
df.loc[df.session_id.isin(df.loc[df['대화명']=='제품 소분류 (11095)', 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["생활가전", "세탁기", "CLDRR"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11005)')&(df['STT'].str.contains('워시')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["생활가전", "세탁기", "WADWS"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11005)'))&(df.STT.isin(df.loc[df['대화명']=='세부 제품 확인 (세탁기)_드럼', 'STT'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["생활가전", "세탁기", "DUDRW"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11005)'))&(df.STT.isin(df.loc[df['대화명']=='세부 제품 확인 (세탁기)_통돌이', 'STT'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["생활가전", "세탁기", "GEDRW"]

# 청소기
df.loc[df.session_id.isin(df.loc[df['대화명'].str.contains('청소기'), 'session_id']), ['PRODUCT_GROUP', '제품']] = ["생활가전", "청소기"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (청소기)_무선', 'session_id']), 'PRODUCT_CODE'] = "A9CLN"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (청소기)_로봇', 'session_id']), 'PRODUCT_CODE'] = "GERBC"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (청소기)_유선', 'session_id']), 'PRODUCT_CODE'] = "GECVC"
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11094)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (청소기)_무선'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["생활가전", "청소기", "A9CLN"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11094)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (청소기)_로봇'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["생활가전", "청소기", "GERBC"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11094)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (청소기)_유선'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["생활가전", "청소기", "GECVC"]
    
# TV
df.loc[df.session_id.isin(df.loc[df['대화명'].str.contains('티비'), 'session_id']), ['PRODUCT_GROUP', '제품']] = ["TV/AV", "티비"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (티비)_긍정', 'session_id']), 'PRODUCT_CODE'] = "OTLED"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (티비)_부정', 'session_id']), 'PRODUCT_CODE'] = "UDLED"
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (티비)_모호', 'session_id']), 'PRODUCT_CODE'] = "UDLED"
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11007)')&(df.STT.str.contains('스탠')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["TV/AV", "티비", "LSLED"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11007)')&(df.STT.str.fullmatch('(빔|프로젝(터|트)|시네| )+')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["TV/AV", "티비", "MOVPJ"]
df.loc[df.session_id.isin(df.loc[((df['대화명']=='제품 소분류 (11007)'))&(df.STT.isin(df.STT[df['대화명']=='세부 제품 확인 (티비)_긍정'].unique())), 'session_id']),\
    ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["TV/AV", "티비", "OTLED"]

# 그외
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (공기청정기)_부정', 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["에어컨/에어케어", "AIARC"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (공기청정기)_모호', 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["에어컨/에어케어", "AIARC"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (공기청정기)_긍정', 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["에어컨/에어케어", "HIDRO"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (가습기)_부정', 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["에어컨/에어케어", "DEHMD"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='세부 제품 확인 (가습기)_긍정', 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["에어컨/에어케어", "HIDRO"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='제품 소분류 (정수기)', 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["주방가전","정수기","HCHWI"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='Q 대상 제품 (휴대폰)', 'session_id']), ['PRODUCT_GROUP','제품','PRODUCT_CODE']] = ['PHONE',"PHONE",'PHONE']
df.loc[df.session_id.isin(df.loc[df['대화명']=='Q 대상 제품 (얼음정수기)', 'session_id']), ['PRODUCT_GROUP','제품']] = ['주방가전','ICEPRFR']
df.loc[df.session_id.isin(df.loc[df['대화명']=='제품 소분류 (모니터)', 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["PC/모니터", "MNT", 'PCMNT']
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11092)')&(df.STT.str.fullmatch('피씨 ?모니터요?')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["PC/모니터", "MNT", "PCMNT"]
df.loc[df.session_id.isin(df.loc[df['대화명']=='제품 소분류 (노트북)', 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["PC/모니터", "노트북", "GRNTB"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11009)')&(df['STT'].str.contains('제습')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["에어컨/에어케어", "DEDEH"]
df.loc[df.session_id.isin(df.loc[(df['대화명'].isin(['세부 제품 확인 (냉장고)_와인셀러)','제품 소분류 (11006)']))&(df.STT.str.contains('와인')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["주방가전", "WIWEF"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11006)')&(df.STT.str.match('(홈 브루|맥주 ?제조기| )+요?')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["주방가전", "BEHBR"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11001)')&(df.STT.str.contains('인덕')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["주방가전", "INELR"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11001)')&(df.STT.str.contains('전기 레인지')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["주방가전", "INELR"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11001)') & (df.STT.str.contains('전기레인지')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["주방가전", "INELR"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11001)') & (df.STT.str.contains('전기렌지')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["주방가전", "INELR"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11001)') & (df.STT.str.contains('식')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["주방가전", "DIDWM"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11001)') & (df.STT.str.contains('전자')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["주방가전", "ERGOR"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11001)') & (df.STT.str.contains('오븐')), 'session_id']), ['PRODUCT_GROUP', 'PRODUCT_CODE']] = ["주방가전", "OVGOR"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='Q 대상 제품 (11001)') & (df.STT.str.match('가스 ?(렌|레인)지')), 'session_id']), ['PRODUCT_GROUP','제품']] = ["주방가전", "GAGRN"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11002)') & (df.STT.str.contains('그램')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["PC/모니터", "노트북", "GRNTB"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11002)') & (df.STT.str.match('((컴|콤)퓨터?|데스크탑?|피씨|일체형| )+(에이에스|상담|서비스)*요?$')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["PC/모니터", "컴퓨터", "MUDSK"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='Q 대상 제품 (11002)') & (df.STT.str.match('.*이어폰')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["PC/모니터", "BTEAR", "N"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11005)') & (df.STT.str.contains('스타일러')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["생활가전", "스타일러", "STDCS"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11005)') & (df.STT.str.contains('안마')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["생활가전", "N", "MAHMC"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='제품 소분류 (11007)') & (df.STT.str.contains('클')), 'session_id']), ['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = ["뷰티/의료기기", "", "CLMHC"]
df.loc[df.session_id.isin(df.loc[(df['대화명']=='Q 대상 제품 (11008)') & (df.STT.str.contains('전화')), 'session_id']), ['제품']] = ["TETEL"]

df[['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']] = df[['PRODUCT_GROUP', '제품', 'PRODUCT_CODE']].fillna("N")

# 여정
df.loc[df.session_id.isin(df.loc[df['대화명']=="렌탈", 'session_id']), "여정"] = "렌탈"
df.loc[df.session_id.isin(df.loc[df['대화명']=="증상_구독분기", 'session_id']), "여정"] = "렌탈"
df.loc[df.session_id.isin(df.loc[df['대화명']=="세척서비스", 'session_id']), "여정"] = "세척서비스"
df.loc[df.session_id.isin(df.loc[df['대화명']=="증상_가전세척분기", 'session_id']), "여정"] = "세척서비스"
df.loc[df.session_id.isin(df.loc[df['대화명']=="이전설치", 'session_id']), "여정"] = "이전설치"
df.loc[df.session_id.isin(df.loc[df['대화명']=="증상_이전설치분기", 'session_id']), "여정"] = "이전설치"
df.loc[df.session_id.isin(df.loc[df['대화명']=="구매", 'session_id']), "여정"] = "구매"
df.loc[df.session_id.isin(df.loc[df['대화명']=="증상_부품구매분기", 'session_id']), "여정"] = "구매"
df.loc[df.session_id.isin(df.loc[df['대화명']=="배송", 'session_id']), "여정"] = "배송"
df.여정 = df.여정.fillna("N")


yymmdd = lambda x:x[2:4]+x[5:7]+x[8:10]
df.to_excel(f"result/voicebot_stt_{yymmdd(df['날짜'].min())}~{yymmdd(df['날짜'].max())}.xlsx", index=False)
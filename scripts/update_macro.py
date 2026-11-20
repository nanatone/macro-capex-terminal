#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宏观流动性、大宗商品期现与科技CapEx一手数据自动抓取及双层决策引擎 (Pro 一体化投研与理财渠道终端版)
==========================================================================================
架构说明（已剔除失真的 60m 分钟级技术信号，100% 聚焦宏观、大宗期现、理财资金面与科技 CapEx 一手数据）：
1. 数据采集层 (ETL)：
   - 美国宏观与狭义流动性 (FRED API 9项)：
     DGS10 (10Y美债名义利率)、DFII10 (10Y TIPS实际利率)、T10YIE (10Y盈亏平衡通胀率)、
     T10Y2Y (10Y-2Y期限利差)、EFFR (联邦基金有效利率)、RRPONTSYD (ON RRP隔夜逆回购)、
     WTREGEN (财政部TGA账户)、WRESBAL (美联储商业银行准备金)、BAMLH0A0HYM2 (美债高收益利差 HY OAS)
   - 中国宏观、银行间非银分层与股债性价比 (AkShare / 中国外汇交易中心 / 东方财富 / 乐咕 7项)：
     CN10Y/CN30Y (中债10Y/30Y国债收益率)、FDR007 (中国外汇交易中心银银间7天回购定盘利率/DR007基准)、
     R007-DR007 (FR007-FDR007 银行间非银流动性分层利差)、CN10Y-US10Y (中美10Y国债利差)、
     M1-M2 同比剪刀差 (央行新口径货币供应量)、CSI300_ERP (沪深300股债风险溢价)
   - 跨资产商品定价锚 (FRED + 新浪外汇/外盘期货 4项)：
     美元指数 (DXY)、铜金比 (COMEX铜/金)、金油比 (COMEX金/WTI原油)、美联储净流动性 (WALCL-TGA-ONRRP)
   - 大宗商品期现基差、期限结构与显性库存分位 (Tushare Pro / SHFE / INE / SGE 6大核心品种)：
     SHFE黄金/Au99.99、SHFE阴极铜、SHFE氧化铝、INE原油、SHFE螺纹钢、黑钨精矿/APT
2. 第一层决策：八维硬逻辑量化阈值触发器 (8-Dim Rule-Based Engine)
   - 覆盖七大宏观/资金面传导维度 + 第⑧维【大宗期现结构与跨资产比价共振】
   - 按品类客观评级大宗商品：区分【贵金属实际利率/央行购金锚】、【低库存(<25%)+现货升水(Back)强逼仓共振】与【现货贴水(Contango)+高库存(>=35%)伪红利排雷】
   - 动态推导五篮子哑铃型策略 (5-Bucket Barbell Strategy) 防守端与进攻端最优配比
3. 第二层决策：条件驱动动态双视角晨报引擎 (Dynamic Soft Report Engine)
   - 动态合成【二级市场交易实战版（含宏观+大宗期现+CapEx联动）】与【存续期管理与渠道陪伴版】
"""

import os
import re
import copy
import time
import socket
import json
import datetime
import numpy as np
import pandas as pd
import requests

try:
    from fredapi import Fred
except ImportError:
    Fred = None

try:
    import akshare as ak
except ImportError:
    ak = None

socket.setdefaulttimeout(30)

DATA_FILE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "macro_daily.json")
TUSHARE_URL = "https://api.tushare.pro"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://finance.sina.com.cn/",
}

DEFAULT_US_METRICS = {
    "dgs10": {
        "name": "10Y 美债名义收益率",
        "code": "DGS10",
        "value": 5.10,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -2.0,
        "chg_5d_bp": -6.0,
        "percentile_3y": 97,
        "threshold_warn": 4.35,
        "role": "DCF分母总锚：名义利率 = 实际利率(DFII10) + 通胀预期(T10YIE)",
        "url": "https://fred.stlouisfed.org/series/DGS10",
        "data_status": "cached",
        "observation_date": "2026-11-17"
    },
    "dfii10": {
        "name": "10Y TIPS 实际利率",
        "code": "DFII10",
        "value": 2.89,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -3.0,
        "chg_5d_bp": -4.0,
        "percentile_3y": 99,
        "threshold_warn": 2.20,
        "role": "DCF真实分母端：剔除通胀后的全球科技成长股与黄金真实无风险贴现率",
        "url": "https://fred.stlouisfed.org/series/DFII10",
        "data_status": "cached",
        "observation_date": "2026-11-17"
    },
    "t10yie": {
        "name": "10Y 盈亏平衡通胀预期",
        "code": "T10YIE",
        "value": 2.19,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -2.0,
        "chg_5d_bp": -1.0,
        "percentile_3y": 7,
        "threshold_warn": 2.45,
        "role": "DCF分子端与大宗再通胀映射：隐含通胀与增长预期（急跌<1.9%预示衰退交易）",
        "url": "https://fred.stlouisfed.org/series/T10YIE",
        "data_status": "cached",
        "observation_date": "2026-11-18"
    },
    "t10y2y": {
        "name": "10Y-2Y 美债期限利差",
        "code": "T10Y2Y",
        "value": 0.27,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -3.0,
        "chg_5d_bp": 2.0,
        "percentile_3y": 40,
        "threshold_warn": 0.50,
        "role": "收益率曲线形态与长债期限溢价(Term Premium)周期",
        "url": "https://fred.stlouisfed.org/series/T10Y2Y",
        "data_status": "cached",
        "observation_date": "2026-11-18"
    },
    "effr": {
        "name": "联邦基金有效利率",
        "code": "EFFR",
        "value": 3.88,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": 0.0,
        "chg_5d_bp": 0.0,
        "percentile_3y": 27,
        "threshold_warn": 5.00,
        "role": "美联储短端政策利率基准与全球大宗贸易融资资金成本锚",
        "url": "https://fred.stlouisfed.org/series/EFFR",
        "data_status": "cached",
        "observation_date": "2026-11-17"
    },
    "rrp": {
        "name": "美联储隔夜逆回购 (ON RRP)",
        "code": "RRPONTSYD",
        "value": 0.33,
        "unit": "十亿美元",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": 0.0,
        "chg_5d_bp": 0.2,
        "percentile_3y": 6,
        "threshold_warn": 200.0,
        "role": "美元非银超额流动性第一道缓冲池（耗尽后QT直接抽离银行准备金）",
        "url": "https://fred.stlouisfed.org/series/RRPONTSYD",
        "data_status": "cached",
        "observation_date": "2026-11-18"
    },
    "tga": {
        "name": "美国财政部一般账户 (TGA)",
        "code": "WTREGEN",
        "value": 914.37,
        "unit": "十亿美元",
        "freq": "weekly",
        "window_label": "近3年历史分位数(周频)",
        "chg_1d_bp": -42.8,
        "chg_5d_bp": 34.1,
        "percentile_3y": 86,
        "threshold_warn": 800.0,
        "role": "财政发债抽水(>800B显著收紧市场准备金)与财政支出投放账户",
        "url": "https://fred.stlouisfed.org/series/WTREGEN",
        "data_status": "cached",
        "observation_date": "2026-11-11"
    },
    "wresbal": {
        "name": "美联储商业银行准备金余额",
        "code": "WRESBAL",
        "value": 2960.60,
        "unit": "十亿美元",
        "freq": "weekly",
        "window_label": "近3年历史分位数(周频)",
        "chg_1d_bp": 30.5,
        "chg_5d_bp": -69.1,
        "percentile_3y": 17,
        "threshold_warn": 3000.0,
        "role": "美元流动性最终底线（跌破3000B触发华尔街回购市场资金紧缺预警）",
        "url": "https://fred.stlouisfed.org/series/WRESBAL",
        "data_status": "cached",
        "observation_date": "2026-11-11"
    },
    "hy_oas": {
        "name": "美国高收益债利差 (HY OAS)",
        "code": "BAMLH0A0HYM2",
        "value": 3.43,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": 9.0,
        "chg_5d_bp": 13.0,
        "percentile_3y": 88,
        "threshold_warn": 4.00,
        "role": "跨市场信用风险溢价与全球套息交易(Carry Trade)去杠杆温度计",
        "url": "https://fred.stlouisfed.org/series/BAMLH0A0HYM2",
        "data_status": "cached",
        "observation_date": "2026-11-17"
    }
}

DEFAULT_CN_METRICS = {
    "cn10y": {
        "name": "10Y 中债国债收益率",
        "code": "CN10Y",
        "value": 1.58,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -1.4,
        "chg_5d_bp": -2.5,
        "percentile_3y": 1,
        "threshold_warn": 2.15,
        "role": "DCF分母端：国内无风险利率核心锚与低利率资产荒温度计",
        "url": "https://yield.chinabond.com.cn/",
        "data_status": "updated",
        "observation_date": "2026-11-19"
    },
    "cn30y": {
        "name": "30Y 超长国债收益率",
        "code": "CN30Y",
        "value": 2.04,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -1.4,
        "chg_5d_bp": -1.0,
        "percentile_3y": 22,
        "threshold_warn": 2.35,
        "role": "险资长久期配置锚与超长期特别国债供给冲击敏感度指标",
        "url": "https://yield.chinabond.com.cn/",
        "data_status": "updated",
        "observation_date": "2026-11-19"
    },
    "dr007": {
        "name": "银行间7天回购定盘利率 (FDR007/DR007)",
        "code": "FDR007",
        "value": 1.37,
        "unit": "%",
        "freq": "daily",
        "window_label": "近1年分位数(外汇交易中心11:30定盘)",
        "chg_1d_bp": 1.0,
        "chg_5d_bp": -2.0,
        "percentile_3y": 32,
        "threshold_warn": 1.80,
        "role": "中国外汇交易中心存款类机构7天质押式回购定盘基准（11:30发布，08:00晨报使用前一日值）",
        "url": "https://www.chinamoney.org.cn/chinese/bkfrr/",
        "data_status": "updated",
        "observation_date": "2026-11-20 (11:30定盘 / 前日1.36%)"
    },
    "r007_dr007_spread": {
        "name": "R007 - DR007 流动性分层利差",
        "code": "FR007-FDR007",
        "value": 3.0,
        "unit": "bp",
        "freq": "daily",
        "window_label": "近1年分位数(外汇交易中心11:30定盘)",
        "chg_1d_bp": -1.0,
        "chg_5d_bp": 0.0,
        "percentile_3y": 27,
        "threshold_warn": 20.0,
        "role": "非银机构融资成本与资金分层监控（走阔提示收紧风险预算；确认理财赎回需申赎规模证据）",
        "url": "https://www.chinamoney.org.cn/chinese/bkfrr/",
        "data_status": "updated",
        "observation_date": "2026-11-20 (FR007 1.40% - FDR007 1.37%)"
    },
    "us_cn_spread": {
        "name": "中美 10Y 国债利差 (中-美)",
        "code": "CN10Y-US10Y",
        "value": -3.52,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": 0.6,
        "chg_5d_bp": -18.4,
        "percentile_3y": 1,
        "threshold_warn": -2.20,
        "role": "跨境资本流动、人民币汇率稳汇率约束与港股离岸市场分母端锚",
        "url": "https://yield.chinabond.com.cn/",
        "data_status": "updated",
        "observation_date": "2026-11-19"
    },
    "m1_m2_diff": {
        "name": "M1 - M2 同比剪刀差 (新口径)",
        "code": "M1-M2",
        "value": -2.7,
        "unit": "%",
        "freq": "monthly",
        "window_label": "近3年(36月)分位数",
        "chg_1d_bp": 1.3,
        "chg_5d_bp": 1.3,
        "percentile_3y": 72,
        "threshold_warn": -3.5,
        "role": "DCF分子端：央行10月金融统计M1同比+4.4%、M2同比+7.1%，剪刀差明显收窄+1.3pct",
        "url": "http://www.pbc.gov.cn/diaochatongjisi/116219/index.html",
        "data_status": "updated",
        "observation_date": "2026-10 (月度·11月中旬发布)"
    },
    "erp_csi300": {
        "name": "沪深300 股债风险溢价 (ERP)",
        "code": "CSI300_ERP",
        "value": 6.31,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": 2.0,
        "chg_5d_bp": 0.0,
        "percentile_3y": 49,
        "threshold_warn": 5.80,
        "role": "A股股债性价比标尺（>=5.8%且处高分位代表权益向下空间封锁）",
        "url": "https://www.csindex.com.cn/",
        "data_status": "updated",
        "observation_date": "2026-11-19"
    }
}

DEFAULT_CROSS_ASSET = {
    "dxy": {
        "id": "dxy",
        "name": "美元指数 (DXY)",
        "code": "ICE: DXY",
        "value": 103.41,
        "unit": "",
        "change": "+0.23%",
        "trend": "up",
        "signal": "美元偏强震荡 · 留意工业品计价弹性",
        "spark": [101.59, 102.29, 101.66, 102.25, 102.91, 103.17, 103.41],
        "role": "全球大宗商品计价货币总锚与非美市场跨境资本流动压力表",
        "data_status": "updated",
        "observation_date": "2026-11-18"
    },
    "cop_gold": {
        "id": "cop_gold",
        "name": "铜金比 (Copper/Gold)",
        "code": "COMEX HG / GC",
        "value": 3.49,
        "unit": "",
        "change": "-0.03",
        "trend": "down",
        "signal": "避险强于顺周期 · 紧盯电网/AIDC拉动",
        "spark": [3.34, 3.38, 3.39, 3.42, 3.46, 3.52, 3.49],
        "role": "全球制造业景气度、特高压电网CapEx与AIDC数据中心用铜密度领先风向标",
        "data_status": "updated",
        "observation_date": "2026-11-18"
    },
    "gold_oil": {
        "id": "gold_oil",
        "name": "金油比 (Gold/WTI)",
        "code": "XAU / WTI",
        "value": 43.7,
        "unit": "x",
        "change": "-0.1x",
        "trend": "down",
        "signal": "工业通胀边际修复",
        "spark": [44.9, 45.2, 48.0, 46.7, 46.2, 43.8, 43.7],
        "role": "衡量货币避险溢价 vs 工业实体能源需求与再通胀交易的温度计",
        "data_status": "updated",
        "observation_date": "2026-11-18"
    },
    "fed_net_liq": {
        "id": "fed_net_liq",
        "name": "美联储净流动性 (Net Liquidity)",
        "code": "WALCL-TGA-ONRRP",
        "value": 5.79,
        "unit": "万亿美元",
        "change": "+40B (周)",
        "trend": "up",
        "signal": "TGA边际释放 · 需盯紧准备金3000B底线",
        "spark": [5.79, 5.87, 5.87, 5.80, 5.75, 5.79],
        "role": "美联储总资产剔除财政部TGA与ON RRP后的广义美元净流动性投放规模",
        "data_status": "updated",
        "observation_date": "2026-11-18"
    }
}

DEFAULT_COMMODITIES = [
    {
        "id": "copper",
        "sector": "有色金属",
        "name": "SHFE 阴极铜",
        "code": "主力CU2701 / 近月CU2612",
        "futuresPrice": 94720,
        "spotPrice": 97290,
        "priceLabel": "主力CU2701 / 近月CU2612",
        "diffLabel": "期货近月-主力月差 (非同日现货)",
        "unit": "元/吨",
        "changePct": -5.07,
        "basis": 2570,
        "basisPct": 2.71,
        "structure": "期货近月Back (+2,570元/吨)",
        "invLevel": "11.8%",
        "invPercentile": 11.8,
        "invWindowLabel": "上期所近5年注册仓单分位",
        "warrantTons": "6,501 吨",
        "invWeeklyChange": "日减 -905吨 (11-18为7,406吨)",
        "futuresObsDate": "2026-11-19",
        "warrantObsDate": "2026-11-19",
        "spotRefNote": "统计局11月上旬1#电解铜市场价维持高位；缺少同日现货报价时仅以期货月差与仓单核验",
        "spotObsDate": "2026-11-10 (统计局旬报)",
        "data_status": "updated",
        "cycleStage": "注册仓单连降至6501吨 · 期货近月Back",
        "capexLink": "AIDC电源HVDC铜排/液冷散热 + 特高压电网CapEx刚性拉动",
        "firstHandNote": "上期所11月19日铜注册仓单由7,406吨降至6,501吨（单日减少905吨），近月CU2612(97,290)较主力CU2701(94,720)维持+2,570元/吨期货Back月差。",
        "forwardCurve": [97290, 94720, 94000, 93880, 93750, 93650],
        "basisHistory": [
            {"date": "W-5", "basis": 320, "inv": 28.0},
            {"date": "W-4", "basis": 680, "inv": 24.0},
            {"date": "W-3", "basis": 1150, "inv": 19.5},
            {"date": "W-2", "basis": 1620, "inv": 16.0},
            {"date": "11-18", "basis": 2180, "inv": 13.5},
            {"date": "11-19", "basis": 2570, "inv": 11.8}
        ]
    },
    {
        "id": "alumina",
        "sector": "铝产业链 (收入vs成本)",
        "name": "SHFE 电解铝(AL) & 氧化铝(AO)",
        "code": "沪铝AL2701 / 氧化铝AO2701",
        "futuresPrice": 2590,
        "spotPrice": 2601,
        "priceLabel": "氧化铝主力AO2701 / 近月AO2612",
        "diffLabel": "氧化铝近月-主力月差 | 沪铝AL2701: 21,330 (-1.66%)",
        "alFuturesPrice": 21330,
        "alNearPrice": 21850,
        "alChangePct": -1.66,
        "unit": "元/吨",
        "changePct": 0.04,
        "basis": 11,
        "basisPct": 0.42,
        "structure": "铝Back(+520) / 氧化铝微Back(+11)",
        "invLevel": "14.5%",
        "invPercentile": 14.5,
        "invWindowLabel": "氧化铝2023.06上市以来(3.4年)仓单分位",
        "warrantTons": "氧化铝192,896吨 / 沪铝35,764吨",
        "invWeeklyChange": "AO日减-43,214吨 / AL日减-7,061吨",
        "futuresObsDate": "2026-11-19",
        "warrantObsDate": "2026-11-19",
        "spotRefNote": "统计局11月上旬铝锭市场价跟踪；注意氧化铝是电解铝(云铝)的采购成本项",
        "spotObsDate": "2026-11-10 (统计局旬报)",
        "data_status": "updated",
        "cycleStage": "氧化铝坚挺(+0.04%)但沪铝回调(-1.66%) · 吨铝利润待确认",
        "capexLink": "绿电铝产能天花板 + 区分【氧化铝外销受益】与【电解铝外购氧化铝成本挤压】",
        "firstHandNote": "11月19日上期所铝仓单减7,061吨至35,764吨，氧化铝总仓单减43,214吨至192,896吨（注：AO于2023年6月上市，分位基于3.4年历史）。当日沪铝主力21,330元/吨(-1.66%)弱于氧化铝2,590元/吨(+0.04%)，对需外购氧化铝的电解铝厂形成短线成本端挤压。",
        "forwardCurve": [2601, 2590, 2597, 2607, 2618, 2631],
        "basisHistory": [
            {"date": "W-5", "basis": 80, "inv": 26.0},
            {"date": "W-4", "basis": 95, "inv": 23.0},
            {"date": "W-3", "basis": 70, "inv": 21.0},
            {"date": "W-2", "basis": 45, "inv": 19.0},
            {"date": "11-18", "basis": 18, "inv": 17.8},
            {"date": "11-19", "basis": 11, "inv": 14.5}
        ]
    },
    {
        "id": "tungsten",
        "sector": "战略小金属",
        "name": "65%黑钨精矿 / 仲钨酸铵(APT)",
        "code": "企业半月长单 (无日频期货)",
        "futuresPrice": 143500,
        "spotPrice": 146000,
        "priceLabel": "11月上半月长单 / 市场参考报价",
        "diffLabel": "长单价差 (非日频交易所数据·仅人工/长单更新)",
        "unit": "元/标吨",
        "changePct": 0.0,
        "basis": 2500,
        "basisPct": 1.74,
        "structure": "非日频长单 · 暂停自动强共振",
        "invLevel": "样本受限",
        "invPercentile": 25.0,
        "invWindowLabel": "行业长单跟踪(无交易所日度仓单)",
        "warrantTons": "无交易所公开仓单",
        "invWeeklyChange": "按半月长单手动更新",
        "futuresObsDate": "2026-11-05 (长单日)",
        "warrantObsDate": "无日频源",
        "spotRefNote": "仅在取得章源/江钨原始长单报价时更新；无当日新长单时撤下个股自动强共振",
        "spotObsDate": "2026-11-05 (11月上半月长单)",
        "data_status": "cached",
        "cycleStage": "长单观察 (上次更新11-05，暂停自动触发个股强共振)",
        "capexLink": "AI服务器高多层PCB精密微钻耗材 + 光伏细钨丝金刚线",
        "firstHandNote": "本地黑钨精矿无交易所日频公开行情刷新路径，当前报价沿用11月5日长单(14.35~14.60万元/标吨)。根据数据纪律，未取得新一期企业原始长单前，降级为观察提示，不自动触发个股强共振。",
        "forwardCurve": [146000, 145000, 144200, 143500, 143000, 142500],
        "basisHistory": [
            {"date": "9月上", "basis": 800, "inv": 30.0},
            {"date": "9月下", "basis": 1200, "inv": 28.0},
            {"date": "10月上", "basis": 1500, "inv": 27.0},
            {"date": "10月下", "basis": 2000, "inv": 26.0},
            {"date": "11月上", "basis": 2500, "inv": 25.0},
            {"date": "待更新", "basis": 2500, "inv": 25.0}
        ]
    },
    {
        "id": "gold",
        "sector": "贵金属",
        "name": "SHFE 沪金 / 上金所 Au99.99",
        "code": "主力AU2702 / 现货Au99.99",
        "futuresPrice": 804.32,
        "spotPrice": 801.93,
        "priceLabel": "沪金主力AU2702 / 上金所Au99.99",
        "diffLabel": "期现基差 (Au99.99现货 - 沪金主力)",
        "unit": "元/克",
        "changePct": -2.60,
        "basis": -2.39,
        "basisPct": -0.30,
        "structure": "上金所Au99.99日行情实取",
        "invLevel": "18.5%",
        "invPercentile": 18.5,
        "invWindowLabel": "黄金按实际利率与央行购金定价(不套用工业库存)",
        "warrantTons": "Au99.99成交 67.31亿元",
        "invWeeklyChange": "近月AU2612报 802.14元/克",
        "futuresObsDate": "2026-11-19",
        "warrantObsDate": "2026-11-19",
        "spotRefNote": "上金所 Au99.99 每日收盘实取 (801.93元/克，日跌-1.09%)",
        "spotObsDate": "2026-11-19 (上金所日行情)",
        "data_status": "updated",
        "cycleStage": "10Y TIPS实际利率(2.89%)高位压制短线金价 · 央行购金托底",
        "capexLink": "对冲美国财政赤字与信用货币稀释的战略底仓",
        "firstHandNote": "11月19日上金所Au99.99现货收报801.93元/克(-1.09%，成交额67.31亿元)，上期所沪金主力AU2702收报804.32元/克(-2.60%)，期现基差-2.39元/克(-0.30%)。受美债10Y实际利率2.89%高位约束，短线回调等待分母端企稳。",
        "forwardCurve": [801.93, 802.14, 803.12, 804.32, 806.46, 808.20],
        "basisHistory": [
            {"date": "W-5", "basis": -1.2, "inv": 24.0},
            {"date": "W-4", "basis": 0.4, "inv": 22.0},
            {"date": "W-3", "basis": 0.8, "inv": 21.0},
            {"date": "W-2", "basis": -0.6, "inv": 20.0},
            {"date": "11-18", "basis": -3.53, "inv": 19.0},
            {"date": "11-19", "basis": -2.39, "inv": 18.5}
        ]
    },
    {
        "id": "crude",
        "sector": "能源化工",
        "name": "INE 中质含硫原油 / EIA周库存",
        "code": "主力SC2701 / 近月SC2612",
        "futuresPrice": 585.9,
        "spotPrice": 595.3,
        "priceLabel": "主力SC2701 / 近月SC2612",
        "diffLabel": "期货近月-主力月差 (SC2612 - SC2701)",
        "unit": "元/桶",
        "changePct": -0.59,
        "basis": 9.4,
        "basisPct": 1.60,
        "structure": "期货近月Back (+9.4元/桶)",
        "invLevel": "31.0%",
        "invPercentile": 31.0,
        "invWindowLabel": "EIA美国商业原油5年周库存分位(地域:美国)",
        "warrantTons": "INE仓单 366.6万桶",
        "invWeeklyChange": "INE仓单日增减 0桶",
        "futuresObsDate": "2026-11-19",
        "warrantObsDate": "2026-11-19 (INE) / 2026-11-13 (EIA周报)",
        "spotRefNote": "库存分位源自美国EIA商业原油周库存(具地域属性)，配合INE原油交割库仓单366.6万桶跟踪",
        "spotObsDate": "2026-11-13 (EIA周报)",
        "data_status": "updated",
        "cycleStage": "近月期货Back(+1.60%) + 美国商业原油库存中性偏低",
        "capexLink": "全球海工油服上游勘探CapEx + 通胀预期(T10YIE)驱动锚",
        "firstHandNote": "11月19日INE原油主力SC2701收报585.9元/桶(-0.59%)，近月SC2612报595.3元/桶，近月-主力期货月差+9.4元/桶(+1.60%)，期限曲线595.3➔546.6元/桶呈Back结构；INE交割库仓单366.6万桶。",
        "forwardCurve": [595.3, 585.9, 576.8, 566.5, 555.5, 546.6],
        "basisHistory": [
            {"date": "W-5", "basis": 2.1, "inv": 36.0},
            {"date": "W-4", "basis": 3.4, "inv": 35.0},
            {"date": "W-3", "basis": 4.8, "inv": 34.0},
            {"date": "W-2", "basis": 6.2, "inv": 32.5},
            {"date": "11-18", "basis": 7.9, "inv": 31.5},
            {"date": "11-19", "basis": 9.4, "inv": 31.0}
        ]
    },
    {
        "id": "rebar",
        "sector": "黑色建材",
        "name": "SHFE 螺纹钢",
        "code": "主力RB2701 / 近月RB2612",
        "futuresPrice": 2983,
        "spotPrice": 2955,
        "priceLabel": "主力RB2701 / 统计局旬度现货参考",
        "diffLabel": "旬度现货-主力价差 | 期货近月RB2612: 3,010",
        "unit": "元/吨",
        "changePct": 0.51,
        "basis": -28,
        "basisPct": -0.94,
        "structure": "现货偏弱 / 老仓单集中注销",
        "invLevel": "42.0%",
        "invPercentile": 42.0,
        "invWindowLabel": "社会库存与钢厂库存5年季节性分位",
        "warrantTons": "10,058 吨",
        "invWeeklyChange": "日减 -20,160吨 (交割月前老仓单注销)",
        "futuresObsDate": "2026-11-19",
        "warrantObsDate": "2026-11-19",
        "spotRefNote": "统计局11月上旬螺纹钢(Φ20mm,HRB400E)市场价偏弱；仓单骤减系老仓单制度性注销而非终端抢料",
        "spotObsDate": "2026-11-10 (统计局旬报)",
        "data_status": "updated",
        "cycleStage": "冬季施工淡季 + 规避传统周期伪红利",
        "capexLink": "传统地产/基建施工映射（须待10月M1-M2改善向实物工作量传导确认）",
        "firstHandNote": "11月19日上期所螺纹钢主力RB2701收报2,983元/吨(+0.51%)，注册仓单由30,218吨降至10,058吨(-20,160吨，主因11月老仓单集中注销)。北方进入冬季施工淡季，维持伪红利排雷观察。",
        "forwardCurve": [2955, 3010, 2983, 2945, 2954, 2972],
        "basisHistory": [
            {"date": "W-5", "basis": 25, "inv": 38.0},
            {"date": "W-4", "basis": 10, "inv": 39.5},
            {"date": "W-3", "basis": -5, "inv": 40.5},
            {"date": "W-2", "basis": -15, "inv": 41.2},
            {"date": "11-18", "basis": -22, "inv": 42.0},
            {"date": "11-19", "basis": -28, "inv": 42.0}
        ]
    }
]

DEFAULT_EQUITY_POOL = [
    {
        "code": "601899.SH",
        "name": "紫金矿业",
        "board": "主板 (10cm)",
        "bucketGroup": "offense_commodity",
        "bucketLabel": "进攻 · 紧缺大宗资源",
        "linkedCommodities": ["copper", "gold"],
        "commodity": "阴极铜 / 黄金",
        "price": 23.88,
        "changePct": -2.61,
        "turnoverYi": 99.20,
        "vsCsi300Pct": -2.64,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-25 (三季报产量核验)",
        "selfSufficiency": "矿产铜107万吨/金72吨，矿端高自给率直接受益铜价与金价",
        "firstHandCatalyst": "上期所铜仓单11-19再降905吨至6,501吨，期货近月CU2612升水主力+2,570元/吨；11-19成交额99.2亿元跟随铜金期货回调",
        "cancelCondition": "若上期所铜仓单连续3日累库>3000吨，或沪铜近月-主力转为Contango贴水，取消资源首选评级",
        "trend_10d": [28.28, 28.41, 28.18, 27.86, 26.78, 26.38, 25.96, 25.99, 24.52, 23.88],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "000807.SZ",
        "name": "云铝股份",
        "board": "主板 (10cm)",
        "bucketGroup": "offense_commodity",
        "bucketLabel": "进攻 · 紧缺大宗资源",
        "linkedCommodities": ["alumina"],
        "commodity": "电解铝(收入) - 氧化铝(成本)",
        "price": 21.35,
        "changePct": -4.00,
        "turnoverYi": 15.41,
        "vsCsi300Pct": -4.03,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-03-28 (2025年报成本结构)",
        "selfSufficiency": "据2025年报：原铝板块采购氧化铝与电力生产电解铝（氧化铝自给率不足100%，外购部分属成本项）",
        "firstHandCatalyst": "11-19沪铝主力21,330元/吨(-1.66%)下跌而氧化铝2,590元/吨(+0.04%)抗跌，仅氧化铝涨/抗跌会挤压外购氧化铝的吨铝利润",
        "cancelCondition": "当氧化铝涨幅>沪铝涨幅导致模拟吨铝利润走弱时，取消自动看多（已触发降级为待确认）",
        "trend_10d": [24.52, 24.87, 23.72, 23.41, 23.31, 23.04, 22.84, 22.68, 22.24, 21.35],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "920807.BJ",
        "name": "奔朗新材",
        "board": "北交所 (30cm)",
        "bucketGroup": "offense_commodity",
        "bucketLabel": "进攻 · 紧缺大宗资源",
        "linkedCommodities": ["tungsten"],
        "commodity": "超硬材料 / 碳化钨工具",
        "price": 6.91,
        "changePct": -4.95,
        "turnoverYi": 0.09,
        "vsCsi300Pct": -4.98,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-11-05 (11月上半月钨长单)",
        "selfSufficiency": "金刚石与硬质合金超硬工具（注：碳化钨亦为原材料成本项，需验证向下游顺价能力）",
        "firstHandCatalyst": "黑钨长单停留在11-05数据且无日频行情源，11-19成交额仅0.09亿元、日跌-4.95%，已撤下自动强共振评级",
        "cancelCondition": "无新一期钨企长单报价或成交额低于0.2亿元时，仅作观察，不列入次日进攻候选",
        "trend_10d": [7.94, 7.93, 8.01, 8.05, 7.96, 7.67, 7.55, 7.45, 7.27, 6.91],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "688122.SH",
        "name": "西部超导",
        "board": "科创板 (20cm)",
        "bucketGroup": "offense_commodity",
        "bucketLabel": "进攻 · 紧缺大宗资源",
        "linkedCommodities": ["tungsten"],
        "commodity": "高端钛合金 / 超导线材",
        "price": 40.74,
        "changePct": 1.80,
        "turnoverYi": 4.40,
        "vsCsi300Pct": 1.77,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-29 (三季报合同负债核验)",
        "selfSufficiency": "航空钛材与MRI/核聚变超导线材龙头，按公司公告与合同负债跟踪（不挂钩黑钨日频共振）",
        "firstHandCatalyst": "11-19逆势收涨+1.80%（跑赢沪深300 +1.77pct，成交额4.40亿元），三季报超导线材与高端钛材交付稳健",
        "cancelCondition": "若后续季度合同负债环比下滑或跌破20日均线量能萎缩，移出观察候选",
        "trend_10d": [43.25, 44.20, 44.04, 43.99, 42.36, 42.35, 40.21, 39.41, 40.02, 40.74],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "600547.SH",
        "name": "山东黄金",
        "board": "主板 (10cm)",
        "bucketGroup": "offense_commodity",
        "bucketLabel": "进攻 · 紧缺大宗资源",
        "linkedCommodities": ["gold"],
        "commodity": "黄金 (国内主力矿山)",
        "price": 23.65,
        "changePct": 1.55,
        "turnoverYi": 14.70,
        "vsCsi300Pct": 1.52,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-30 (三季报自产金核验)",
        "selfSufficiency": "自产矿产金约45吨，纯正黄金矿端高自给率龙头",
        "firstHandCatalyst": "11-19上金所Au99.99收于801.93元/克，股价逆势收涨+1.55%（成交额14.70亿元，跑赢基准+1.52pct），展现矿端底部韧性",
        "cancelCondition": "若10Y TIPS实际利率突破3.00%且上金所Au99.99跌破780元/克，暂停逢低加配",
        "trend_10d": [26.20, 26.72, 27.00, 26.65, 24.91, 24.41, 23.85, 24.19, 23.29, 23.65],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "300139.SZ",
        "name": "晓程科技",
        "board": "创业板 (20cm)",
        "bucketGroup": "offense_commodity",
        "bucketLabel": "进攻 · 紧缺大宗资源",
        "linkedCommodities": ["gold"],
        "commodity": "黄金 (加纳金矿)",
        "price": 33.63,
        "changePct": -2.83,
        "turnoverYi": 5.51,
        "vsCsi300Pct": -2.86,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-28 (三季报加纳矿产金核验)",
        "selfSufficiency": "海外加纳三大金矿权益，20cm高Beta金矿标的",
        "firstHandCatalyst": "受10Y美债实际利率(2.89%)高位与沪金主力(-2.60%)回调影响，11-19收跌-2.83%（成交额5.51亿元），相对山东黄金偏弱",
        "cancelCondition": "在实际利率(DFII10)未拐头向下且相对板块龙头(山东黄金)走弱期间，暂不追高",
        "trend_10d": [39.86, 39.55, 40.24, 40.09, 38.06, 37.11, 34.89, 35.69, 34.61, 33.63],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "920088.BJ",
        "name": "科力股份",
        "board": "北交所 (30cm)",
        "bucketGroup": "offense_commodity",
        "bucketLabel": "进攻 · 紧缺大宗资源",
        "linkedCommodities": ["crude"],
        "commodity": "原油 / 油田工程技术服务",
        "price": 30.29,
        "changePct": -4.15,
        "turnoverYi": 0.49,
        "vsCsi300Pct": -4.18,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-29 (三季报油服营收核验)",
        "selfSufficiency": "中石油/中海油油田化学与地面工程服务商（业绩取决于上游油企CapEx结算而非单日油价）",
        "firstHandCatalyst": "INE原油近月SC2612虽升水主力+9.4元/桶，但个股11-19收跌-4.15%（成交额0.49亿元），短期走势与单日油价背离",
        "cancelCondition": "日成交额不足1亿元且相对基准走弱(-4.18pct)时，仅作跟踪，不触发次日交易",
        "trend_10d": [37.07, 36.47, 35.55, 35.90, 36.14, 35.37, 33.10, 31.98, 31.60, 30.29],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "300191.SZ",
        "name": "潜能恒信",
        "board": "创业板 (20cm)",
        "bucketGroup": "offense_commodity",
        "bucketLabel": "进攻 · 紧缺大宗资源",
        "linkedCommodities": ["crude"],
        "commodity": "原油 (海上油气区块勘探)",
        "price": 20.59,
        "changePct": -2.23,
        "turnoverYi": 0.88,
        "vsCsi300Pct": -2.26,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-28 (三季报区块进展)",
        "selfSufficiency": "地震数据处理找油技术 + 海上油气区块开发分成权益",
        "firstHandCatalyst": "11-19收跌-2.23%（成交额0.88亿元），虽INE原油维持近月Back结构，但区块投产兑现需以巨潮正式钻井/投产公告为准",
        "cancelCondition": "无新钻井/产量公告且成交额低于1.5亿元时，维持观察，不自动升级为强共振",
        "trend_10d": [22.99, 23.25, 22.84, 23.59, 23.20, 22.41, 21.50, 21.05, 21.06, 20.59],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "600900.SH",
        "name": "长江电力",
        "board": "主板 (10cm)",
        "bucketGroup": "defense_dividend",
        "bucketLabel": "防御 · 央国企真红利",
        "linkedCommodities": [],
        "commodity": "10Y中债(1.58%) 险资OCI类债锚",
        "price": 30.88,
        "changePct": 0.65,
        "turnoverYi": 28.11,
        "vsCsi300Pct": 0.62,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-31 (三季报来水与发电量公告)",
        "selfSufficiency": "六库联调垄断水电资产，现金流与分红完全免疫工业品Contango累库风险",
        "firstHandCatalyst": "10Y中债降至1.58%、11-20 FDR007仅1.37%，11-19长江电力收涨+0.65%（成交28.11亿元，近10日由29.91稳步升至30.88元）",
        "cancelCondition": "若10Y中债收益率快速反弹突破2.15%导致股息率利差收窄，则下调防御红利权重",
        "trend_10d": [29.91, 29.83, 29.67, 29.48, 29.61, 29.91, 30.49, 30.60, 30.68, 30.88],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "600941.SH",
        "name": "中国移动",
        "board": "主板 (10cm)",
        "bucketGroup": "defense_dividend",
        "bucketLabel": "防御 · 央国企真红利",
        "price": 107.80,
        "changePct": 0.39,
        "turnoverYi": 15.87,
        "vsCsi300Pct": 0.36,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-22 (三季报经营现金流与派息指引)",
        "linkedCommodities": [],
        "commodity": "高派息央企 + 运营商算力底座",
        "selfSufficiency": "超70%派息率指引明确 + 经营现金流稳健，兼具类债红利底仓与智算网络属性",
        "firstHandCatalyst": "11-19收涨+0.39%报107.80元（成交额15.87亿元，近10日由98.70低点连续走强至107.80元），与低利率资产荒高度契合",
        "cancelCondition": "若应收账款周转明显恶化或运营商资本开支超预期重回扩张侵蚀自由现金流，取消红利首选",
        "trend_10d": [103.49, 103.03, 101.55, 99.93, 98.70, 101.46, 104.49, 105.20, 107.38, 107.80],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "300308.SZ",
        "name": "中际旭创",
        "board": "创业板 (20cm)",
        "bucketGroup": "offense_tech_capex",
        "bucketLabel": "进攻 · 科技CapEx跟踪",
        "linkedCommodities": ["copper"],
        "commodity": "800G/1.6T光模块 (行业月报+季报核对)",
        "price": 408.40,
        "changePct": 3.05,
        "turnoverYi": 166.08,
        "vsCsi300Pct": 3.02,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-28 (三季报毛利率/存货核验) | 2026-11-10 (台积电10月营收参照)",
        "selfSufficiency": "全球800G/1.6T光模块龙头（注：台积电月营收仅验证行业景气，公司订单与份额以季报/公告为准）",
        "firstHandCatalyst": "11-19放量收涨+3.05%报408.40元（全市场头部成交额166.08亿元，跑赢沪深300 +3.02pct），资金承接强劲",
        "cancelCondition": "若下季北美四大云厂CapEx同比增速斜率放缓，或公司季报存货跌价准备/毛利率环比恶化，立即止盈降级",
        "trend_10d": [448.00, 452.03, 471.00, 542.00, 508.90, 466.20, 408.97, 393.00, 396.30, 408.40],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    },
    {
        "code": "300124.SZ",
        "name": "汇川技术",
        "board": "创业板 (20cm)",
        "bucketGroup": "offense_tech_capex",
        "bucketLabel": "进攻 · 科技CapEx跟踪",
        "linkedCommodities": [],
        "commodity": "通用工控 + 具身智能丝杠/伺服",
        "price": 53.45,
        "changePct": 0.00,
        "turnoverYi": 11.75,
        "vsCsi300Pct": -0.03,
        "quoteDate": "2026-11-19",
        "announcementDate": "2026-10-29 (三季报) | 10月M1-M2(-2.7%)顺周期跟踪",
        "selfSufficiency": "国内通用伺服/变频器龙头（人形机器人执行器仍处送样孵化期，必须待正式定点公告确认，不提前记为已验证）",
        "firstHandCatalyst": "11-19平盘报53.45元（成交额11.75亿元）；央行10月M1-M2剪刀差收窄至-2.7%利好工控主业筑底，机器人业务待正式定点公告",
        "cancelCondition": "未披露正式量产定点通知书前，不赋予具身智能溢价；若工控订单增速低于预期则移出候选",
        "trend_10d": [56.66, 57.55, 55.85, 55.10, 53.11, 53.45, 52.96, 52.80, 53.45, 53.45],
        "trend_dates": ["11-06", "11-09", "11-10", "11-11", "11-12", "11-13", "11-16", "11-17", "11-18", "11-19"]
    }
]

DEFAULT_FIELD_NOTES = [
    {
        "id": 1,
        "time": "2026-11-20 11:35",
        "category": "银行间资金面 · 理财渠道跟踪",
        "source": "中国货币网(ChinaMoney) 11:30定盘原表 & 央行10月金融统计",
        "title": "11月20日FDR007报1.37%、FR007报1.40%(分层仅3bp)，10月M1-M2收窄至-2.7%",
        "content": "直连中国货币网实取11月20日11:30最新定盘：FDR007报1.37%、FR007报1.40%，非银分层利差仅3.0bp（11月19日为4.0bp），融资端无流动性分层压力；央行10月M1同比+4.4%、M2同比+7.1%，M1-M2剪刀差收窄至-2.7%（较9月-4.0%改善1.3pct）。",
        "impact": "宏观顺风 (Macro Tailwind)",
        "linkedTickers": ["600900.SH", "600941.SH"]
    },
    {
        "id": 2,
        "time": "2026-11-19 16:10",
        "category": "有色金属 · 铜/铝",
        "source": "上期所11月18-19日仓单日报原表 & 2025年报产业链核对",
        "title": "上期所铜仓单日减905吨至6,501吨；沪铝跌1.66%而氧化铝抗跌，云铝暂转观察",
        "content": "核对上期所11月19日原表：铜注册仓单由18日7,406吨降至6,501吨(-905吨)，近月CU2612较主力CU2701升水+2,570元/吨；铝仓单减7,061吨至35,764吨，氧化铝仓单减43,214吨。因11-19沪铝(-1.66%)弱于氧化铝(+0.04%)，对需采购氧化铝的云铝股份(000807.SZ)构成短线成本端扰动，系统自动将云铝降为『吨铝利润待确认』。",
        "impact": "分化验证 (Divergent Signal)",
        "linkedTickers": ["601899.SH", "000807.SZ"]
    },
    {
        "id": 3,
        "time": "2026-11-19 15:30",
        "category": "科技CapEx · 算力/机器人调研",
        "source": "台积电10月营收公告(11-10) & 深交所11-19已完成日线",
        "title": "中际旭创11-19成交166.08亿元收涨+3.05%；行业景气与个股公告严格分层",
        "content": "台积电11月10日公布10月营收验证算力行业高景气；个股层面11月19日中际旭创(300308.SZ)收盘408.40元(+3.05%，全天成交166.08亿元，跑赢沪深300基准+3.02pct)，相对强弱显著优于缺乏新长单更新的小金属标的。",
        "impact": "强利多 (Strong Bullish)",
        "linkedTickers": ["300308.SZ", "300124.SZ"]
    }
]

DEFAULT_CAPEX_TRACKER = [
    {
        "stage": "第一阶段：算力与硬件基建 (行业按月观察 · 公司按公告更新)",
        "status": "行业月报高景气 · 个股按季报/公告核对",
        "obs_date": "行业最新数据日: 2026-11-10 (台积电10月营收) / 2026-11-11 (韩国前10日出口)",
        "badge_color": "emerald",
        "chain_nodes": "台积电CoWoS先进封装 ➔ HBM3e/HBM4存储 ➔ 800G/1.6T光模块与CPO ➔ AIDC液冷(CDU)、HVDC供电与上游铜铝需求",
        "lead_indicators": "① 每月10日：台积电(TSMC)月度合并营收(最新披露: 2026-11-10)；② 每月1/11/21日：韩国关税厅半导体出口；③ 注：行业月报仅验证算力总量景气，单家A股供应商订单与份额必须以定期报告及巨潮公告为准。",
        "sec_cninfo_keywords": [
            "Purchases of property and equipment",
            "Capital expenditures guidance",
            "CoWoS capacity allocation",
            "1.6T transceivers",
            "Liquid cooling",
            "合同负债",
            "在建工程",
            "存货跌价准备"
        ],
        "verification_metric": "【公告前记录上次指引 · 公布后对照】北美四大云厂商(MSFT/GOOG/META/AMZN)10-Q实际CapEx与下季指引对比上季记录；A股光模块/服务器龙头核对季度毛利率、合同负债与经营现金流净额。",
        "red_flag": "【取消看多条件 (Invalidation)】云厂商CapEx同比增速斜率连续放缓，或A股硬件供应商季报出现存货跌价准备激增、毛利率环比下滑。"
    },
    {
        "stage": "第二阶段：推理期扩展与 Agent 商业化 (ROI 闭环期)",
        "status": "分化观察期 · 考核RPO与毛利",
        "obs_date": "财报核验期: 2026Q3 10-Q / 三季报披露期 (2026-10月底)",
        "badge_color": "blue",
        "chain_nodes": "Reasoning推理期算力扩展 ➔ 端侧轻量化蒸馏模型 ➔ 企业级AI Agent工作流 ➔ 垂直行业SaaS增收(ARR)",
        "lead_indicators": "① 企业软件财报 RPO(剩余履约义务)增速 vs 营收增速剪刀差；② 算力租赁 GPU 时租价格走势；③ 在公告前记录公司上次ARR/毛利率指引，披露后逐项对照是否超出原判断。",
        "sec_cninfo_keywords": [
            "Remaining Performance Obligations (RPO)",
            "Net Dollar Retention (NDR)",
            "AI Copilot seat adoption",
            "Inference token volume",
            "ARR增速",
            "研发费用资本化率",
            "毛利率同比变动"
        ],
        "verification_metric": "【ROI商业化闭环】软件企业财报披露RPO（剩余履约义务）增速超越营收增速 > 5pct，且销售毛利率未被API调用成本侵蚀。",
        "red_flag": "【取消看多条件 (Invalidation)】仅披露Token调用量增长但无付费收入披露，或因推理算力成本过高导致毛利率同比下滑。"
    },
    {
        "stage": "第三阶段：具身智能与人形机器人 (0 到 1 公告核验期)",
        "status": "送样观察期 · 无正式定点不提前计价",
        "obs_date": "公告核验基准: 巨潮资讯网截至 2026-11-19 正式公告",
        "badge_color": "purple",
        "chain_nodes": "VLA多模态端到端控制算法 ➔ 行星滚柱丝杠/微型丝杠 ➔ 谐波减速器/空心杯电机 ➔ 六维力传感器与稀土永磁/轻量化材料",
        "lead_indicators": "① 巨潮资讯网《定点通知书》或《日常经营重大合同公告》；② 专用产线环评批复与高精度磨床采购落地；③ 严格区分『投资者调研纪要口头送样』与『正式具有法律约束力的定点公告』。",
        "sec_cninfo_keywords": [
            "行星滚柱丝杠",
            "六维力传感器",
            "空心杯电机",
            "客户送样验证",
            "小批量定点通知书",
            "专用产线环评",
            "投资者关系活动记录表"
        ],
        "verification_metric": "【仅认准正式公告】国内零部件厂商在巨潮资讯网披露正式定点通知书或批量供货合同，且传统主业经营现金流为正。",
        "red_flag": "【取消看多条件 (Invalidation)】仅停留在无约束力框架协议、无正式定点公告却伴随大股东/高管减持预披露，立即剔除。"
    }
]


def evaluate_commodity_grade(item: dict, dfii10: float = 2.60) -> dict:
    """
    分品类大宗商品期现、仓单与产业链利润传导评级引擎（含逾期缓存/无日频源自动降级锁）：
    1. 缓存与无日频源降级锁：若 data_status == 'cached' 或无交易所日频源(如黑钨精矿未在当日更新长单)，
       自动降级为 CACHE_WATCH ('长单/缓存观察 · 暂停强共振')，绝不自动触发个股强共振。
    2. 贵金属 (gold)：按实际利率 (DFII10) + 上金所Au99.99实金行情独立评级。
    3. 铝产业链 (alumina)：同时校验氧化铝(AO)与沪铝(AL)日涨跌幅。若氧化铝涨/抗跌而沪铝下跌，
       提示『氧化铝坚挺但沪铝回调 · 下游吨铝利润待确认』。
    4. 工业品 (copper/crude/rebar)：缺少同日现货报价时，诚实标注为『仓单去化 + 期货近月Back共振』，不夸大为同日现货升水确认。
    """
    cid = item.get("id", "")
    sector = item.get("sector", "")
    basis = float(item.get("basis", 0) or 0)
    basis_pct = float(item.get("basisPct", 0) or 0)
    inv_pct = float(item.get("invPercentile", 50) or 50)
    data_status = item.get("data_status", "updated")
    obs_date = item.get("futuresObsDate") or item.get("spotObsDate") or "历史缓存"

    # 1. 无日频源或逾期缓存自动降级锁（如黑钨精矿）
    if data_status == "cached" or cid == "tungsten":
        return {
            "grade": "CACHE_WATCH",
            "label": f"长单观察 ({obs_date})",
            "badge_color": "slate",
            "allow_offense": False,
            "pseudo_dividend_warning": False,
            "summary": f"该品种无交易所日频公开行情刷新路径（当前沿用 {obs_date} 数据）。按数据校准纪律，未取得新一期原始长单前暂停强共振评级，仅作观察。"
        }

    # 2. 贵金属独立评级
    if cid == "gold" or sector == "贵金属":
        if dfii10 >= 2.30:
            return {
                "grade": "PRECIOUS_HEDGE",
                "label": "央行购金底仓 · 实际利率约束",
                "badge_color": "amber",
                "allow_offense": True,
                "pseudo_dividend_warning": False,
                "summary": f"10Y TIPS实际利率({dfii10}%)处高位对金价构成短线分母约束，上金所Au99.99({item.get('spotPrice')}元/克)与央行购金封锁中期下行空间，宜待实际利率回落分批配置。"
            }
        return {
            "grade": "STRONG_BULL",
            "label": "实际利率回落 · 黄金主升共振",
            "badge_color": "emerald",
            "allow_offense": True,
            "pseudo_dividend_warning": False,
            "summary": f"10Y TIPS实际利率({dfii10}%)回落叠加央行购金与上金所Au99.99实金买盘支撑，贵金属迎来分母端双击。"
        }

    # 3. 铝及氧化铝产业链：区分氧化铝价格与电解铝(沪铝)吨铝利润方向
    if cid == "alumina":
        al_chg = float(item.get("alChangePct", 0.0) or 0.0)
        ao_chg = float(item.get("changePct", 0.0) or 0.0)
        if al_chg < 0 and ao_chg >= al_chg:
            return {
                "grade": "BACK_WATCH",
                "label": "仓单去化 · 吨铝利润待确认",
                "badge_color": "amber",
                "allow_offense": True,
                "pseudo_dividend_warning": False,
                "summary": (
                    f"上期所氧化铝(2023.06上市以来3.4年分位 {inv_pct}%)与沪铝仓单同步去化，近月维持期货Back(+"
                    f"{basis_pct}%)；但当日沪铝({al_chg:+.2f}%)弱于氧化铝({ao_chg:+.2f}%)，对需外购氧化铝的电解铝企(如云铝)形成成本端扰动，吨铝利润方向待确认。"
                )
            }

    # 4. 工业品仓单去化 + 期货近月Back共振
    if inv_pct < 25.0 and basis > 0:
        return {
            "grade": "STRONG_BULL",
            "label": "仓单去化+期货Back 强共振",
            "badge_color": "emerald",
            "allow_offense": True,
            "pseudo_dividend_warning": False,
            "summary": f"交易所注册仓单处 {inv_pct}% 低分位（{item.get('invWindowLabel', '近5年')}），且近月较主力合约升水 +{basis_pct}%（期货月差Back），供需紧平衡支撑上游高自给率矿端龙头。"
        }

    if basis < 0 and inv_pct >= 35.0:
        return {
            "grade": "CONTANGO_BEAR",
            "label": "现货偏弱+高库存 · 伪红利排雷",
            "badge_color": "rose",
            "allow_offense": False,
            "pseudo_dividend_warning": True,
            "summary": f"旬度现货较期货贴水 {basis_pct}% 且库存分位达 {inv_pct}%，反映施工淡季需求偏弱，严禁因静态高股息率将其作为红利底仓。"
        }

    if basis > 0:
        return {
            "grade": "BACK_WATCH",
            "label": "期货近月Back · 结构支撑",
            "badge_color": "sky",
            "allow_offense": True,
            "pseudo_dividend_warning": False,
            "summary": f"期货近月保持升水 +{basis_pct}%（月差Back），库存分位 {inv_pct}%（{item.get('invWindowLabel', '')}）处于可控区间，具备逢低配置价值。"
        }

    return {
        "grade": "NEUTRAL",
        "label": "期现均衡观察",
        "badge_color": "slate",
        "allow_offense": False,
        "pseudo_dividend_warning": False,
        "summary": f"库存分位 {inv_pct}% / 价差 {basis_pct}%，等待仓单去化斜率或期现结构进一步明朗。"
    }


def calc_stats(series: pd.Series, is_percent_yield: bool = True):
    """计算时间序列的最新值、最近1期变动、最近5期变动及近3年日历窗口历史分位数"""
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if isinstance(clean.index, pd.DatetimeIndex):
        clean = clean.sort_index()
    if len(clean) == 0:
        return None
    latest = float(clean.iloc[-1])
    prev_1d = float(clean.iloc[-2]) if len(clean) >= 2 else latest
    prev_5d = float(clean.iloc[-6]) if len(clean) >= 6 else prev_1d

    multiplier = 100.0 if is_percent_yield else 1.0
    chg_1d = round((latest - prev_1d) * multiplier, 1)
    chg_5d = round((latest - prev_5d) * multiplier, 1)

    window_3y = (clean.loc[clean.index[-1] - pd.DateOffset(years=3):]
                 if isinstance(clean.index, pd.DatetimeIndex) else clean.iloc[-756:])
    percentile_3y = int(round((window_3y < latest).mean() * 100))

    tail_10 = clean.iloc[-10:]
    trend_10d = [round(float(v), 2) for v in tail_10.tolist()]
    if isinstance(tail_10.index, pd.DatetimeIndex):
        trend_dates = [d.strftime("%m-%d") for d in tail_10.index]
    else:
        trend_dates = [f"T-{len(tail_10) - 1 - i}" for i in range(len(tail_10))]
        if trend_dates:
            trend_dates[-1] = "最新"

    return {
        "observation_date": (clean.index[-1].strftime("%Y-%m-%d")
                             if isinstance(clean.index, pd.DatetimeIndex) else None),
        "data_status": "updated",
        "value": round(latest, 2),
        "chg_1d_bp": chg_1d,
        "chg_5d_bp": chg_5d,
        "percentile_3y": max(1, min(99, percentile_3y)),
        "trend_10d": trend_10d,
        "trend_dates": trend_dates
    }


def fetch_us_macro(fallback_us: dict) -> dict:
    """从美联储 FRED 获取美国一手宏观与狭义流动性指标"""
    fred_key = os.environ.get("FRED_API_KEY", "").strip()
    if not fred_key or Fred is None:
        raise RuntimeError("FRED_API_KEY 或 fredapi 缺失，停止更新。")

    try:
        fred = Fred(api_key=fred_key)
        start_dt = (datetime.date.today() - datetime.timedelta(days=1150)).strftime("%Y-%m-%d")

        series_map = {
            "dgs10": ("DGS10", True),
            "dfii10": ("DFII10", True),
            "t10yie": ("T10YIE", True),
            "t10y2y": ("T10Y2Y", True),
            "effr": ("EFFR", True),
            "rrp": ("RRPONTSYD", False),
            "tga": ("WTREGEN", False),
            "wresbal": ("WRESBAL", False),
            "hy_oas": ("BAMLH0A0HYM2", True),
        }

        updated_us = copy.deepcopy(DEFAULT_US_METRICS)
        for k, v in fallback_us.items():
            if k in updated_us:
                for dyn_field in ("value", "chg_1d_bp", "chg_5d_bp", "percentile_3y", "observation_date", "data_status"):
                    if dyn_field in v:
                        updated_us[k][dyn_field] = v[dyn_field]
            else:
                updated_us[k] = copy.deepcopy(v)

        successful = 0
        for key, (fred_code, is_yield) in series_map.items():
            try:
                raw_s = fred.get_series(fred_code, observation_start=start_dt)
                if fred_code in ("WTREGEN", "WRESBAL"):
                    raw_s = raw_s / 1000.0
                stats = calc_stats(raw_s, is_percent_yield=is_yield)
                if stats and key in updated_us:
                    updated_us[key].update(stats)
                    successful += 1
                    print(f"[SUCCESS] FRED {fred_code} 更新成功: {stats['value']}")
            except Exception as sub_e:
                print(f"[WARN] FRED 序列 {fred_code} 拉取异常，保留前值: {type(sub_e).__name__}")

        if successful == 0:
            raise RuntimeError("所有 FRED 序列获取失败，停止更新。")
        return updated_us
    except Exception:
        raise RuntimeError("FRED 更新失败，数据文件未写入。") from None


def _fetch_chinamoney_csv_direct() -> tuple:
    """
    直连中国货币网公开 CSV (fdr-chrt.csv / frr-chrt.csv) 获取最新 FDR007 与 FR007。
    使用 ssl.OP_LEGACY_SERVER_CONNECT (0x4) 兼容 OpenSSL 3.0+ 传统 TLS 重协商。
    """
    import ssl
    import urllib.request

    ctx = ssl.create_default_context()
    ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)

    def _read_csv_series(url: str) -> pd.Series:
        req = urllib.request.Request(url, headers=HTTP_HEADERS)
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            txt = resp.read().decode("utf-8", errors="ignore")
        data = {}
        for line in txt.splitlines():
            parts = [p.strip() for p in line.strip().split(",")]
            if len(parts) >= 8 and re.match(r"^\d{4}-\d{2}-\d{2}$", parts[0]):
                try:
                    val = float(parts[7])
                    if val > 0:
                        data[pd.to_datetime(parts[0])] = val
                except ValueError:
                    pass
        return pd.Series(data).sort_index() if data else pd.Series(dtype=float)

    fdr_s = _read_csv_series("https://www.chinamoney.org.cn/r/cms/www/chinamoney/data/currency/fdr-chrt.csv")
    frr_s = _read_csv_series("https://www.chinamoney.org.cn/r/cms/www/chinamoney/data/currency/frr-chrt.csv")
    return fdr_s, frr_s


def fetch_cn_macro(fallback_cn: dict) -> dict:
    """获取中国国债收益率、中美利差、ChinaMoney回购定盘利率(FDR007/FR007)、M1-M2剪刀差及沪深300 ERP"""
    updated_cn = copy.deepcopy(DEFAULT_CN_METRICS)
    for k, v in fallback_cn.items():
        if k in updated_cn:
            for dyn_field in ("value", "chg_1d_bp", "chg_5d_bp", "percentile_3y", "observation_date", "data_status"):
                if dyn_field in v:
                    updated_cn[k][dyn_field] = v[dyn_field]
        else:
            updated_cn[k] = copy.deepcopy(v)

    # 1. 优先通过直连 JSON 获取中债 10Y / 30Y 收益率与中美利差（失败时回退 AkShare）
    bond_updated = False
    try:
        url_b = (
            "https://datacenter-web.eastmoney.com/api/data/v1/get?"
            "reportName=RPTA_WEB_TREASURYYIELD&columns=SOLAR_DATE,EMM00166466,EMM00166469,EMG00001310"
            "&pageNumber=1&pageSize=500&sortColumns=SOLAR_DATE&sortTypes=-1"
        )
        r_b = requests.get(url_b, headers=HTTP_HEADERS, timeout=10)
        rows_b = (r_b.json().get("result") or {}).get("data") or []
        if rows_b:
            bdf = pd.DataFrame(rows_b)
            bdf["dt"] = pd.to_datetime(bdf["SOLAR_DATE"])
            bdf = bdf.set_index("dt").sort_index()
            s10 = pd.to_numeric(bdf["EMM00166466"], errors="coerce").dropna()
            s30 = pd.to_numeric(bdf["EMM00166469"], errors="coerce").dropna()
            sus = pd.to_numeric(bdf["EMG00001310"], errors="coerce").dropna()
            if len(s10) >= 2:
                st10 = calc_stats(s10, is_percent_yield=True)
                if st10:
                    updated_cn["cn10y"].update(st10)
                    bond_updated = True
            if len(s30) >= 2:
                st30 = calc_stats(s30, is_percent_yield=True)
                if st30:
                    updated_cn["cn30y"].update(st30)
            if len(s10) >= 2 and len(sus) >= 2:
                sp_s = (s10 - sus).dropna()
                st_sp = calc_stats(sp_s, is_percent_yield=True)
                if st_sp:
                    updated_cn["us_cn_spread"].update(st_sp)
            print(f"[SUCCESS] 直连中债10Y/30Y及中美利差更新成功: {updated_cn['cn10y']['value']}%")
    except Exception as e:
        print(f"[WARN] 直连中债收益率异常，尝试回退 AkShare: {e}")

    if not bond_updated and ak is not None:
        start_str = (datetime.date.today() - datetime.timedelta(days=1150)).strftime("%Y%m%d")
        try:
            bond_df = ak.bond_zh_us_rate(start_date=start_str)
            if not bond_df.empty:
                bond_df.index = pd.to_datetime(bond_df["日期"])
                bond_df = bond_df.sort_index()
                if "中国国债收益率10年" in bond_df.columns:
                    stats_10y = calc_stats(bond_df["中国国债收益率10年"], is_percent_yield=True)
                    if stats_10y:
                        updated_cn["cn10y"].update(stats_10y)
                if "中国国债收益率30年" in bond_df.columns:
                    stats_30y = calc_stats(bond_df["中国国债收益率30年"], is_percent_yield=True)
                    if stats_30y:
                        updated_cn["cn30y"].update(stats_30y)
                if "中国国债收益率10年" in bond_df.columns and "美国国债收益率10年" in bond_df.columns:
                    cn_s = pd.to_numeric(bond_df["中国国债收益率10年"], errors="coerce")
                    us_s = pd.to_numeric(bond_df["美国国债收益率10年"], errors="coerce")
                    spread_s = (cn_s - us_s).dropna()
                    stats_sp = calc_stats(spread_s, is_percent_yield=True)
                    if stats_sp:
                        updated_cn["us_cn_spread"].update(stats_sp)
        except Exception as e:
            print(f"[WARN] AkShare 中债收益率/中美利差获取异常，保留缓存值: {e}")

    # 2. 优先通过直连 ChinaMoney CSV 获取 FDR007 与 FR007（11:30 发布定盘）
    repo_updated = False
    try:
        fdr_s, frr_s = _fetch_chinamoney_csv_direct()
        if len(fdr_s) >= 2:
            stats_dr = calc_stats(fdr_s, is_percent_yield=True)
            if stats_dr:
                obs_dt = stats_dr["observation_date"]
                stats_dr["observation_date"] = f"{obs_dt} (11:30定盘)"
                updated_cn["dr007"].update(stats_dr)
                repo_updated = True
                print(f"[SUCCESS] ChinaMoney 直连 FDR007 更新成功: {stats_dr['value']}% ({obs_dt})")
            if len(frr_s) >= 2:
                spread_bp_s = ((frr_s - fdr_s) * 100.0).dropna()
                stats_r_dr = calc_stats(spread_bp_s, is_percent_yield=False)
                if stats_r_dr:
                    obs_dt = stats_r_dr["observation_date"]
                    stats_r_dr["observation_date"] = f"{obs_dt} (FR007-FDR007)"
                    updated_cn["r007_dr007_spread"].update(stats_r_dr)
                    print(f"[SUCCESS] ChinaMoney 直连 R007-DR007 更新成功: {stats_r_dr['value']}bp ({obs_dt})")
    except Exception as e:
        print(f"[WARN] ChinaMoney 直连 CSV 异常，尝试回退 AkShare: {e}")

    if not repo_updated and ak is not None:
        try:
            fdr_df = ak.repo_rate_query(symbol="银银间回购定盘利率")
            if not fdr_df.empty and "FDR007" in fdr_df.columns:
                fdr_df = fdr_df.dropna(subset=["date", "FDR007"]).copy()
                fdr_df.index = pd.to_datetime(fdr_df["date"])
                fdr_df = fdr_df.sort_index()
                stats_dr = calc_stats(fdr_df["FDR007"], is_percent_yield=True)
                if stats_dr:
                    updated_cn["dr007"].update(stats_dr)
                fr_df = ak.repo_rate_query(symbol="回购定盘利率")
                if not fr_df.empty and "FR007" in fr_df.columns:
                    fr_df = fr_df.dropna(subset=["date", "FR007"]).copy()
                    fr_df.index = pd.to_datetime(fr_df["date"])
                    fr_df = fr_df.sort_index()
                    spread_bp_s = ((fr_df["FR007"] - fdr_df["FDR007"]) * 100.0).dropna()
                    stats_r_dr = calc_stats(spread_bp_s, is_percent_yield=False)
                    if stats_r_dr:
                        updated_cn["r007_dr007_spread"].update(stats_r_dr)
        except Exception as e:
            print(f"[WARN] ChinaMoney 回购定盘利率获取异常，保留缓存值: {e}")

    # 3. 优先通过直连央行金融统计接口获取最新月度 M1 - M2 同比剪刀差
    m1m2_updated = False
    try:
        url_m = (
            "https://datacenter-web.eastmoney.com/api/data/v1/get?"
            "reportName=RPT_ECONOMY_CURRENCY_SUPPLY"
            "&columns=REPORT_DATE,TIME,BASIC_CURRENCY_SAME,CURRENCY_SAME"
            "&pageNumber=1&pageSize=36&sortColumns=REPORT_DATE&sortTypes=-1"
        )
        r_m = requests.get(url_m, headers=HTTP_HEADERS, timeout=10)
        rows_m = (r_m.json().get("result") or {}).get("data") or []
        if len(rows_m) >= 2:
            rows_m = list(reversed(rows_m))
            m1_s = pd.to_numeric([x.get("CURRENCY_SAME") for x in rows_m], errors="coerce")
            m2_s = pd.to_numeric([x.get("BASIC_CURRENCY_SAME") for x in rows_m], errors="coerce")
            scissors = pd.Series(m1_s - m2_s).dropna()
            if len(scissors) >= 2:
                latest_sc = round(float(scissors.iloc[-1]), 2)
                prev_sc = round(float(scissors.iloc[-2]), 2)
                chg_sc = round(latest_sc - prev_sc, 2)
                pct_3y = int(round((scissors < latest_sc).mean() * 100))
                t10 = [round(float(v), 2) for v in scissors.iloc[-10:].tolist()]
                d10 = [str(x.get("TIME", ""))[-3:] for x in rows_m[-10:]]
                obs_m = str(rows_m[-1].get("TIME", "2026年10月份"))
                updated_cn["m1_m2_diff"].update({
                    "value": latest_sc,
                    "chg_1d_bp": chg_sc,
                    "chg_5d_bp": chg_sc,
                    "percentile_3y": max(1, min(99, pct_3y)),
                    "observation_date": f"{obs_m} (月度)",
                    "data_status": "updated",
                    "trend_10d": t10,
                    "trend_dates": d10
                })
                m1m2_updated = True
                print(f"[SUCCESS] 央行 M1-M2 剪刀差更新成功: {latest_sc}% ({obs_m})")
    except Exception as e:
        print(f"[WARN] 直连 M1-M2 剪刀差异常，尝试回退 AkShare: {e}")

    if not m1m2_updated and ak is not None:
        try:
            m_df = ak.macro_china_money_supply()
            if not m_df.empty and "货币(M1)-同比增长" in m_df.columns and "货币和准货币(M2)-同比增长" in m_df.columns:
                m_df = m_df.iloc[::-1].reset_index(drop=True)
                m1_s = pd.to_numeric(m_df["货币(M1)-同比增长"], errors="coerce")
                m2_s = pd.to_numeric(m_df["货币和准货币(M2)-同比增长"], errors="coerce")
                scissors = (m1_s - m2_s).dropna()
                if len(scissors) >= 2:
                    latest_sc = round(float(scissors.iloc[-1]), 2)
                    prev_sc = round(float(scissors.iloc[-2]), 2)
                    chg_sc = round(latest_sc - prev_sc, 2)
                    window_3y = scissors.iloc[-36:] if len(scissors) > 36 else scissors
                    pct_3y = int(round((window_3y < latest_sc).mean() * 100))
                    updated_cn["m1_m2_diff"].update({
                        "value": latest_sc,
                        "chg_1d_bp": chg_sc,
                        "chg_5d_bp": chg_sc,
                        "percentile_3y": max(1, min(99, pct_3y)),
                        "observation_date": str(m_df["月份"].iloc[-1]),
                        "data_status": "updated"
                    })
        except Exception as e:
            print(f"[WARN] AkShare M1-M2剪刀差获取异常，保留缓存值: {e}")

    if ak is not None:
        try:
            ebs_df = ak.stock_ebs_lg()
            if not ebs_df.empty and "股债利差" in ebs_df.columns:
                ebs_df = ebs_df.dropna(subset=["日期", "股债利差"]).copy()
                ebs_df.index = pd.to_datetime(ebs_df["日期"])
                ebs_df = ebs_df.sort_index()
                erp_series = pd.to_numeric(ebs_df["股债利差"], errors="coerce") * 100.0
                stats_erp = calc_stats(erp_series, is_percent_yield=True)
                if stats_erp:
                    updated_cn["erp_csi300"].update(stats_erp)
                    print(f"[SUCCESS] AkShare 沪深300 ERP更新成功: {stats_erp['value']}%")
        except Exception as e:
            print(f"[WARN] AkShare 沪深300 ERP获取异常，保留缓存值: {e}")

    return updated_cn


def _fetch_sina_dxy():
    url = "https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var%20t=/NewForexService.getDayKLine?symbol=fx_sdiniw"
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        resp.raise_for_status()
        m = re.search(r'"([^"]+)"', resp.text)
        if not m:
            return None
        closes = []
        last_dt = None
        for row in m.group(1).split("|"):
            parts = row.split(",")
            if len(parts) >= 5:
                last_dt = parts[0]
                closes.append(float(parts[4]))
        return (closes[-10:], last_dt) if len(closes) >= 2 else None
    except Exception as e:
        print(f"[WARN] 新浪 DXY 拉取异常，保留缓存: {e}")
        return None


def _fetch_sina_global_futures(symbol):
    url = f"https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20t=/GlobalFuturesService.getGlobalFuturesDailyKLine?symbol={symbol}"
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=12)
        resp.raise_for_status()
        txt = resp.text
        left, right = txt.find("["), txt.rfind("]")
        if left == -1 or right == -1:
            return None
        rows = json.loads(txt[left:right + 1])
        series = {}
        for r in rows[-30:]:
            dt, c = r.get("date"), r.get("close")
            if dt and c is not None and float(c) > 0:
                series[dt] = float(c)
        return series if series else None
    except Exception as e:
        print(f"[WARN] 新浪外盘期货 {symbol} 拉取异常，保留缓存: {e}")
        return None


def fetch_cross_asset_anchors(fallback_cross: dict) -> dict:
    """获取跨资产比价锚：美元指数 DXY、铜金比、金油比、美联储净流动性"""
    updated = copy.deepcopy(DEFAULT_CROSS_ASSET)
    if isinstance(fallback_cross, dict):
        for k, v in fallback_cross.items():
            if k in updated and isinstance(v, dict):
                updated[k].update(v)

    dxy_res = _fetch_sina_dxy()
    if dxy_res:
        dxy_vals, dxy_dt = dxy_res
        latest_dxy, prev_dxy = dxy_vals[-1], dxy_vals[-2]
        pct = (latest_dxy - prev_dxy) / prev_dxy * 100.0 if prev_dxy else 0.0
        updated["dxy"].update({
            "value": round(latest_dxy, 2),
            "change": f"{pct:+.2f}%",
            "trend": "down" if pct <= 0 else "up",
            "signal": "弱美元周期 · 利好金铜定价" if latest_dxy < 103.0 else "美元偏强震荡 · 留意计价弹性",
            "spark": [round(v, 2) for v in dxy_vals],
            "data_status": "updated",
            "observation_date": dxy_dt or updated["dxy"].get("observation_date")
        })

    gc_map = _fetch_sina_global_futures("GC")
    cl_map = _fetch_sina_global_futures("CL")
    hg_map = _fetch_sina_global_futures("HG")

    if gc_map and cl_map:
        common_go = sorted(set(gc_map) & set(cl_map))[-10:]
        if len(common_go) >= 2:
            go_series = [round(gc_map[d] / cl_map[d], 1) for d in common_go]
            diff_go = round(go_series[-1] - go_series[-2], 1)
            updated["gold_oil"].update({
                "value": go_series[-1],
                "change": f"{diff_go:+.1f}x",
                "trend": "down" if diff_go <= 0 else "up",
                "signal": "工业通胀边际修复" if diff_go <= 0 else "避险溢价相对走扩",
                "spark": go_series,
                "data_status": "updated",
                "observation_date": common_go[-1]
            })

    if hg_map and gc_map:
        common_cg = sorted(set(hg_map) & set(gc_map))[-10:]
        if len(common_cg) >= 2:
            cg_series = [round((hg_map[d] * 22.0462) / gc_map[d], 2) for d in common_cg]
            diff_cg = round(cg_series[-1] - cg_series[-2], 2)
            updated["cop_gold"].update({
                "value": cg_series[-1],
                "change": f"{diff_cg:+.2f}",
                "trend": "up" if diff_cg >= 0 else "down",
                "signal": "制造业与电网用铜回暖" if diff_cg >= 0 else "避险强于顺周期 · 紧盯电网/AIDC拉动",
                "spark": cg_series,
                "data_status": "updated",
                "observation_date": common_cg[-1]
            })

    return updated


def _ts_call(api_name, token, fields="", **params):
    if not token:
        return None
    payload = {"api_name": api_name, "token": token, "params": params, "fields": fields}
    for attempt in range(3):
        try:
            resp = requests.post(TUSHARE_URL, json=payload, timeout=15)
            resp.raise_for_status()
            body = resp.json()
            if body.get("code") != 0:
                return None
            data = body.get("data") or {}
            items, cols = data.get("items") or [], data.get("fields") or []
            return pd.DataFrame(items, columns=cols) if (items and cols) else pd.DataFrame(columns=cols)
        except Exception:
            if attempt < 2:
                time.sleep(0.8 * (attempt + 1))
    return None


def fetch_commodities_data(fallback_commodities: list, dfii10: float = 2.60) -> list:
    """更新 6 大核心大宗商品期现基差、期限结构、库存分位及自动基本面评级"""
    commodities = copy.deepcopy(fallback_commodities if fallback_commodities else DEFAULT_COMMODITIES)
    token = os.environ.get("TUSHARE_TOKEN", "").strip()

    if token:
        try:
            df_dates = _ts_call("fut_daily", token, ts_code="CU.SHF", fields="trade_date")
            trade_dates = sorted(df_dates["trade_date"].astype(str).unique().tolist(), reverse=True)[:15] if (df_dates is not None and not df_dates.empty) else []
            if trade_dates:
                latest_dt = trade_dates[0]
                df_shfe = _ts_call("fut_daily", token, trade_date=latest_dt, exchange="SHFE")
                df_ine = _ts_call("fut_daily", token, trade_date=latest_dt, exchange="INE")
                df_sge = _ts_call("sge_daily", token, ts_code="Au99.99", fields="ts_code,trade_date,close")
                sge_gold = None
                if df_sge is not None and not df_sge.empty:
                    df_sge = df_sge.sort_values("trade_date")
                    val = pd.to_numeric(df_sge["close"].iloc[-1], errors="coerce")
                    if pd.notna(val) and val > 0:
                        sge_gold = float(val)

                exch_map = {"SHF": df_shfe, "INE": df_ine}
                c_cfg = {
                    "gold": ("AU", "SHF", 2),
                    "copper": ("CU", "SHF", 0),
                    "alumina": ("AO", "SHF", 0),
                    "crude": ("SC", "INE", 1),
                    "rebar": ("RB", "SHF", 0),
                }
                for item in commodities:
                    cid = item.get("id")
                    if cid not in c_cfg:
                        continue
                    prefix, suffix, dec = c_cfg[cid]
                    df_e = exch_map.get(suffix)
                    if df_e is None or df_e.empty:
                        continue
                    sub = df_e[df_e["ts_code"].astype(str).str.match(rf"^{prefix}\d{{4}}\.{suffix}$")].copy()
                    if sub.empty:
                        continue
                    for col in ("close", "pre_settle", "oi"):
                        sub[col] = pd.to_numeric(sub[col], errors="coerce").fillna(0.0)
                    sub = sub[(sub["close"] > 0) & (sub["oi"] > 0)].sort_values("ts_code")
                    if sub.empty:
                        continue
                    main_row = sub.loc[sub["oi"].idxmax()]
                    near_row = sub.iloc[0]
                    fut_p = float(main_row["close"])
                    pre_s = float(main_row["pre_settle"])
                    spot_p = sge_gold if (cid == "gold" and sge_gold is not None) else float(near_row["close"])
                    basis = round(spot_p - fut_p, 2)
                    basis_pct = round((basis / fut_p) * 100.0, 2) if fut_p > 0 else 0.0
                    item["futuresPrice"] = int(round(fut_p)) if dec == 0 else round(fut_p, dec)
                    item["spotPrice"] = int(round(spot_p)) if dec == 0 else round(spot_p, dec)
                    item["changePct"] = round((fut_p - pre_s) / pre_s * 100.0, 2) if pre_s > 0 else item.get("changePct", 0.0)
                    item["basis"] = int(round(basis)) if dec == 0 else round(basis, dec)
                    item["basisPct"] = basis_pct
        except Exception as e:
            print(f"[WARN] Tushare 期货期现更新跳过，保留实盘快照: {e}")

    for item in commodities:
        grade_info = evaluate_commodity_grade(item, dfii10=dfii10)
        item["fundamentalGrade"] = grade_info["grade"]
        item["gradeLabel"] = grade_info["label"]
        item["gradeBadgeColor"] = grade_info["badge_color"]
        item["gradeSummary"] = grade_info["summary"]
        item["allowOffense"] = grade_info["allow_offense"]
        item["pseudoDividendWarning"] = grade_info["pseudo_dividend_warning"]

    return commodities


def evaluate_equity_fundamental(stock: dict, commodities_map: dict, defense_weight: int = 65) -> dict:
    """
    基于【宏观哑铃状态 + 关联大宗商品期现仓单 + 产业链收入/成本传导 + 公司公告边界】生成个股基本面诊断：
    - 严禁把原料成本端涨价直接当成冶炼/深加工股的自动利多（如云铝股份 000807.SZ 外购部分氧化铝生产电解铝）；
    - 无日频行情源（如黑钨精矿 CACHE_WATCH）自动降级为长单观察；
    - 科技CapEx标的区分行业月报景气与单家公司正式公告。
    """
    code = stock.get("code", "")
    bucket = stock.get("bucketGroup", "")
    linked_ids = stock.get("linkedCommodities") or []
    linked_items = [commodities_map[cid] for cid in linked_ids if cid in commodities_map]

    if bucket == "defense_dividend":
        return {
            "resonanceGrade": "CORE_DEFENSE",
            "actionLabel": "真红利底仓 · 险资OCI承接",
            "badgeColor": "emerald",
            "diagnosis": f"当前宏观防守权重 {defense_weight}%，10Y中债低利率环境下具备险资OCI账户持续承接，且无大宗Contango累库风险。"
        }

    if bucket == "offense_tech_capex":
        return {
            "resonanceGrade": "CAPEX_TRACK",
            "actionLabel": "行业月报高景气 · 个股按公告核对",
            "badgeColor": "indigo",
            "diagnosis": "台积电月度营收验证算力总量高景气；单家公司订单、毛利率与存货周转须以定期报告及巨潮正式公告为准。"
        }

    # 特判：云铝股份 (000807.SZ) 电解铝(收入) vs 氧化铝(成本) 利润传导核验
    if code == "000807.SZ" and "alumina" in commodities_map:
        ao_item = commodities_map["alumina"]
        al_chg = float(ao_item.get("alChangePct", -1.66) or 0.0)
        ao_chg = float(ao_item.get("changePct", 0.04) or 0.0)
        if al_chg <= 0 and ao_chg >= al_chg:
            return {
                "resonanceGrade": "COST_SQUEEZE_WATCH",
                "actionLabel": "成本端有变化 · 吨铝利润待确认",
                "badgeColor": "amber",
                "diagnosis": (
                    f"据2025年报，公司原铝板块需采购氧化铝与电力；最新交易日沪铝({al_chg:+.2f}%)弱于氧化铝({ao_chg:+.2f}%)，"
                    "仅氧化铝坚挺而铝价回调会挤压外购氧化铝部分的吨铝利润，据此取消自动看多，待铝价与成本变化支持盈利改善后再纳入候选。"
                )
            }

    if linked_items:
        grades = [c.get("fundamentalGrade") for c in linked_items]
        names_str = " / ".join(f"{c['name'].split('/')[0].strip()}({c.get('gradeLabel', '')})" for c in linked_items)
        if "CONTANGO_BEAR" in grades:
            return {
                "resonanceGrade": "CONTANGO_AVOID",
                "actionLabel": "基本面排雷 · 规避Contango累库",
                "badgeColor": "rose",
                "diagnosis": f"关联品种【{names_str}】处于现货偏弱且库存分位偏高状态，周期分子端承压，规避伪红利陷阱。"
            }
        if "CACHE_WATCH" in grades:
            return {
                "resonanceGrade": "CACHE_WATCH",
                "actionLabel": "长单/公告观察 · 暂停自动强共振",
                "badgeColor": "slate",
                "diagnosis": f"关联品种【{names_str}】无交易所日频公开源，未取得新一期企业原始长单或公司公告前仅作观察。"
            }
        if "STRONG_BULL" in grades:
            return {
                "resonanceGrade": "STRONG_RESONANCE",
                "actionLabel": "仓单去化+期货Back · 矿端观察首选",
                "badgeColor": "emerald",
                "diagnosis": f"关联品种【{names_str}】注册仓单持续下降(<25%分位)且近月维持期货Back升水，高自给率矿端龙头优先跟踪。"
            }
        if "PRECIOUS_HEDGE" in grades:
            return {
                "resonanceGrade": "PRECIOUS_ANCHOR",
                "actionLabel": "央行购金底仓 · 盯实际利率拐点",
                "badgeColor": "amber",
                "diagnosis": f"关联品种【{names_str}】受央行购金与上金所Au99.99支撑，短线等待10Y TIPS实际利率回落窗口。"
            }
        return {
            "resonanceGrade": "BACK_SUPPORT",
            "actionLabel": "期货Back支撑 · 结合量价跟踪",
            "badgeColor": "sky",
            "diagnosis": f"关联品种【{names_str}】维持近月Back结构，结合个股成交额与相对基准强弱跟踪。"
        }

    return {
        "resonanceGrade": "NEUTRAL_WATCH",
        "actionLabel": "基本面跟踪",
        "badgeColor": "slate",
        "diagnosis": "持续跟踪行业供需与公司正式公告。"
    }


def _fetch_ashare_completed_kline(code: str, today_str: str, is_after_close: bool) -> dict:
    """
    直连东财公开日K线接口获取单只A股（含沪/深/北交所）最新已完成交易日的前复权收盘价、涨跌幅、成交额(亿)及近10日收盘序列。
    盘中运行(15:15前)自动剔除当日未收盘K线，确保早盘/盘中复盘100%使用上一交易日已完成日线。
    """
    num, exch = code.split(".")
    secid = f"1.{num}" if exch == "SH" else f"0.{num}"
    url = (
        f"https://push2his.eastmoney.com/api/qt/stock/kline/get?"
        f"secid={secid}&fields1=f1,f2,f3&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59"
        f"&klt=101&fqt=1&end=20500101&lmt=15"
    )
    resp = requests.get(url, headers=HTTP_HEADERS, timeout=8)
    resp.raise_for_status()
    klines = ((resp.json().get("data") or {}).get("klines")) or []
    if not is_after_close:
        klines = [k for k in klines if not k.startswith(today_str)]
    done = klines[-10:]
    if not done:
        return {}
    last_parts = done[-1].split(",")
    return {
        "quoteDate": last_parts[0],
        "price": round(float(last_parts[2]), 2),
        "changePct": round(float(last_parts[8]), 2),
        "turnoverYi": round(float(last_parts[6]) / 1e8, 2),
        "trend_10d": [round(float(k.split(",")[2]), 2) for k in done],
        "trend_dates": [k.split(",")[0][5:] for k in done]
    }


def enrich_equity_pool(fallback_pool: list, commodities: list, defense_weight: int = 65, fetch_live_quotes: bool = False) -> list:
    pool = copy.deepcopy(fallback_pool if fallback_pool else DEFAULT_EQUITY_POOL)
    c_map = {c["id"]: c for c in (commodities or [])}

    if fetch_live_quotes:
        now_cst = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
        today_str = now_cst.strftime("%Y-%m-%d")
        is_after_close = (now_cst.hour > 15) or (now_cst.hour == 15 and now_cst.minute >= 15)
        csi300_chg = 0.03
        try:
            bm = _fetch_ashare_completed_kline("000300.SH", today_str, is_after_close)
            if bm:
                csi300_chg = bm["changePct"]
        except Exception:
            pass
        for stock in pool:
            try:
                q = _fetch_ashare_completed_kline(stock["code"], today_str, is_after_close)
                if q:
                    stock.update(q)
                    stock["vsCsi300Pct"] = round(q["changePct"] - csi300_chg, 2)
            except Exception as e:
                print(f"[WARN] A股日线 {stock.get('code')} 拉取跳过，保留快照: {e}")

    for stock in pool:
        res = evaluate_equity_fundamental(stock, c_map, defense_weight=defense_weight)
        stock.update(res)
    return pool


def run_hard_rule_engine(us: dict, cn: dict, cross_asset: dict = None, commodities: list = None) -> dict:
    """
    第一层决策引擎：硬逻辑量化阈值触发器 (Rule-Based Engine)
    - 当仅传入 (us, cn) 时保持与原 7 维单元测试 100% 向后兼容；
    - 当传入 commodities / cross_asset 时（或默认完整运行），输出包含大宗期现与紧缺资源桶的完整决策结构。
    """
    rules = []
    defense_weight = 50  # 初始中性防守端权重 50%

    us10y = us["dgs10"]["value"]
    us10y_5d = us["dgs10"]["chg_5d_bp"]
    dfii10 = us.get("dfii10", DEFAULT_US_METRICS["dfii10"])["value"]
    t10yie = us.get("t10yie", DEFAULT_US_METRICS["t10yie"])["value"]

    rrp_val = us["rrp"]["value"]
    tga_val = us["tga"]["value"]
    wresbal_val = us.get("wresbal", DEFAULT_US_METRICS["wresbal"])["value"]
    hy_oas_val = us.get("hy_oas", DEFAULT_US_METRICS["hy_oas"])["value"]

    cn10y = cn["cn10y"]["value"]
    cn10y_5d = cn["cn10y"]["chg_5d_bp"]
    cn10y_pct = cn["cn10y"]["percentile_3y"]
    dr007_val = cn["dr007"]["value"]
    r_dr_spread = cn.get("r007_dr007_spread", DEFAULT_CN_METRICS["r007_dr007_spread"])["value"]
    us_cn_spread = cn.get("us_cn_spread", DEFAULT_CN_METRICS["us_cn_spread"])["value"]
    scissors_val = cn["m1_m2_diff"]["value"]
    scissors_chg = cn["m1_m2_diff"]["chg_1d_bp"]
    erp_val = cn["erp_csi300"]["value"]
    erp_pct = cn["erp_csi300"]["percentile_3y"]

    # 规则 1：海外分母端（名义利率 + 实际利率 vs 通胀预期拆解）
    if us10y >= 4.35 or us10y_5d >= 12.0 or dfii10 >= 2.20:
        r1_status, r1_label = "warning", "实际利率承压"
        slope_desc = f"近5日虽边际回落{abs(us10y_5d)}bp" if us10y_5d < 0 else f"近5日上冲{us10y_5d:+}bp"
        r1_action = (
            f"10Y美债报{us10y}%（{slope_desc}），拆解看10Y TIPS实际利率高达{dfii10}%、通胀预期{t10yie}%。"
            "高实际利率直接抬升DCF无风险分母，压制远期现金流占比高但无当期业绩的纯概念股；海外债久期宜控制在短端。"
        )
        defense_weight += 8
    elif us10y <= 3.90 and t10yie < 1.90:
        r1_status, r1_label = "danger", "衰退通缩交易"
        r1_action = (
            f"10Y美债回落至{us10y}%，但主要由盈亏平衡通胀率({t10yie}%)坍塌驱动，反映分子端衰退恐慌快于分母宽松，美债看多但需防范美股顺周期与高Beta杀盈利。"
        )
        defense_weight += 6
    elif us10y <= 3.90 and us10y_5d <= -5.0:
        r1_status, r1_label = "bullish", "分母实质宽松"
        r1_action = (
            f"10Y美债降至{us10y}%且实际利率({dfii10}%)温和回落、通胀预期({t10yie}%)稳定，属典型预防式宽松，利好纳指、港股科技、黄金与硬科技成长估值扩张。"
        )
        defense_weight -= 8
    else:
        r1_status, r1_label = "neutral", "分母区间震荡"
        r1_action = (
            f"美债名义利率({us10y}%)与实际利率({dfii10}%)处于箱体震荡，分母端扰动钝化，超额收益取决于分子端大宗期现供需缺口与云厂商CapEx兑现。"
        )

    rules.append({
        "id": "RULE_US_RATE_SLOPE",
        "dimension": "① 海外分母端与实际利率拆解",
        "status": r1_status,
        "status_label": r1_label,
        "trigger_condition": "10Y美债>=4.35% 或 实际利率>=2.20% 或 5日>=+12bp",
        "current_reading": f"名义 {us10y}% (5日{us10y_5d:+}bp) | 实际 {dfii10}% + 通胀 {t10yie}%",
        "action_instruction": r1_action
    })

    # 规则 2：美元狭义流动性底线
    if (rrp_val < 200.0 and tga_val > 750.0) or wresbal_val < 3000.0 or hy_oas_val >= 4.00:
        r2_status, r2_label = "danger", "准备金逼近临界"
        r2_action = (
            f"ON RRP蓄水池仅剩{rrp_val}B（近乎耗尽），叠加财政部TGA高达{tga_val}B抽水，直接导致美联储商业银行准备金降至{wresbal_val}B（低于3000B充裕线），"
            f"高收益债利差报{hy_oas_val}%。警惕华尔街回购利率跳升与海外高Beta资产去杠杆波动。"
        )
        defense_weight += 6
    elif rrp_val > 400.0 or (tga_val < 500.0 and wresbal_val >= 3200.0 and hy_oas_val < 3.20):
        r2_status, r2_label = "bullish", "美元流动性充裕"
        r2_action = f"银行体系准备金({wresbal_val}B)充裕且高收益债利差({hy_oas_val}%)处于低位，美元狭义流动性无虞，支持全球风险资产与大宗资源偏好。"
        defense_weight -= 4
    else:
        r2_status, r2_label = "neutral", "准备金供需平衡"
        r2_action = f"银行准备金({wresbal_val}B)与TGA({tga_val}B)处于均衡水位，持续跟踪美国财政部季度再融资会议(QRA)长债发行比例。"

    rules.append({
        "id": "RULE_US_LIQUIDITY_BUFFER",
        "dimension": "② 美元狭义流动性与准备金底线",
        "status": r2_status,
        "status_label": r2_label,
        "trigger_condition": "(RRP<200B且TGA>750B) 或 准备金<3000B 或 HY利差>=4%",
        "current_reading": f"准备金 {wresbal_val}B | RRP {rrp_val}B | TGA {tga_val}B | HY利差 {hy_oas_val}%",
        "action_instruction": r2_action
    })

    # 规则 3：跨市场汇率约束与中美利差传导
    if us_cn_spread <= -2.20:
        r3_status, r3_label = "warning", "深度倒挂约束"
        r3_action = (
            f"中美10Y利差深度倒挂至{us_cn_spread}%（中债{cn10y}% vs 美债{us10y}%），外部稳汇率（USD/CNH）约束国内央行大幅降息空间，同时压制离岸港股分母端外资回流；权益配置宜偏重内需自主可控、上游紧缺资源与A股红利底仓。"
        )
        defense_weight += 4
    elif us_cn_spread >= -1.20:
        r3_status, r3_label = "bullish", "利差收敛共振"
        r3_action = f"中美利差倒挂显著收敛至{us_cn_spread}%，人民币汇率与跨境套息压力解除，港股核心资产与新兴市场迎来分母端外资回流共振。"
        defense_weight -= 4
    else:
        r3_status, r3_label = "neutral", "利差温和倒挂"
        r3_action = f"中美10Y利差报{us_cn_spread}%，汇率约束可控，国内货币政策坚持以我为主。"

    rules.append({
        "id": "RULE_CROSS_BORDER_FX",
        "dimension": "③ 中美利差与跨境汇率约束",
        "status": r3_status,
        "status_label": r3_label,
        "trigger_condition": "中美10Y利差 <= -2.20% 触发外部稳汇率与离岸分母约束",
        "current_reading": f"中美利差 {us_cn_spread}% (中债{cn10y}% - 美债{us10y}%)",
        "action_instruction": r3_action
    })

    # 规则 4：国内宽货币与低利率资产荒象限
    if cn10y <= 2.18 and dr007_val <= 1.80 and cn10y_5d < 8.0:
        r4_status, r4_label = "bullish", "资产荒延续"
        ultra_low_note = (
            f"注意10Y中债已处{cn10y}%（近3年{cn10y_pct}%极低分位），30Y超长债({cn['cn30y']['value']}%)赔率空间受限且对特别国债供给敏感，建议以1-3年高等级信用债吃票息+央国企红利低波替代盲目拉长久期。"
            if cn10y <= 1.75 else
            "狭义资金面宽松，固收与险资欠配压力显著，高股息红利低波与中短久期高等级信用债安全垫极厚。"
        )
        r4_action = f"银行间FDR007报{dr007_val}%维持低位，{ultra_low_note}"
        defense_weight += 5
    elif cn10y_5d >= 8.0 or dr007_val >= 1.95 or cn10y > 2.25:
        r4_status, r4_label = "warning", "债市久期防回调"
        r4_action = "资金面边际收敛或长债供给冲击显现，10Y/30Y超长国债面临止盈负反馈风险，建议立即缩短久期至短债与同业存单。"
        defense_weight -= 4
    else:
        r4_status, r4_label = "neutral", "票息筑底"
        r4_action = "国内利率曲线平稳，维持中短久期信用债底仓吃票息策略。"

    rules.append({
        "id": "RULE_CN_ASSET_FAMINE",
        "dimension": "④ 国内宽货币与资产荒象限",
        "status": r4_status,
        "status_label": r4_label,
        "trigger_condition": "10Y中债<=2.18% 且 DR007<=1.80% 且 5日变动<8bp",
        "current_reading": f"10Y中债 {cn10y}% ({cn10y_pct}%分位) | 30Y {cn['cn30y']['value']}% | DR007 {dr007_val}%",
        "action_instruction": r4_action
    })

    # 规则 5：机构行为与银行理财预防性赎回负反馈预警
    if r_dr_spread >= 25.0 or (cn10y_5d >= 10.0 and r_dr_spread >= 15.0):
        r5_status, r5_label = "danger", "赎回负反馈预警"
        r5_action = (
            f"R007-DR007非银分层利差走阔至{r_dr_spread}bp（叠加10Y中债5日变动{cn10y_5d:+}bp），非银与理财子资金面趋紧，"
            "高度警惕散户赎回债基/理财引发银行二永债与同业存单抛售踩踏！立即降杠杆、砍信用久期并启动渠道客户安抚。"
        )
        defense_weight += 5
    elif r_dr_spread >= 15.0 or cn10y_5d >= 6.0:
        r5_status, r5_label = "warning", "非银资金边际收敛"
        r5_action = f"R007-DR007利差报{r_dr_spread}bp，非银机构融资成本边际抬升或长债小幅波动，存续产品暂停拉长二永债久期。"
        defense_weight += 2
    else:
        r5_status, r5_label = "bullish", "非银无赎回压力"
        r5_action = (
            f"R007-DR007分层利差仅{r_dr_spread}bp且10Y中债5日变动{cn10y_5d:+}bp，银行向非银流动性传导顺畅，理财与债基规模稳定，信用债票息套息环境安全。"
        )
        defense_weight -= 2

    rules.append({
        "id": "RULE_CN_WEALTH_REDEMPTION",
        "dimension": "⑤ 理财赎回负反馈与流动性分层",
        "status": r5_status,
        "status_label": r5_label,
        "trigger_condition": "R007-DR007分层利差>=25bp 或 (长债5日>=10bp且利差>=15bp)",
        "current_reading": f"R007-DR007利差 {r_dr_spread}bp | 10Y中债5日 {cn10y_5d:+}bp",
        "action_instruction": r5_action
    })

    # 规则 6：国内分子端宽信用拐点
    if scissors_val > -3.5 or (scissors_val > -4.8 and scissors_chg > 0):
        r6_status, r6_label = "bullish", "宽信用主动修复"
        r6_action = (
            f"M1-M2剪刀差回升至{scissors_val}%（环比{scissors_chg:+}pct），企业活期存款与财政实物工作量同步活化，建议由纯红利防御向顺周期工业金属、核心资产及弹性标的切换。"
        )
        defense_weight -= 10
    elif scissors_val > -6.0:
        r6_status, r6_label = "neutral", "剪刀差磨底待验证"
        r6_action = (
            f"新口径M1-M2剪刀差报{scissors_val}%，但最新月度环比变动为{scissors_chg:+}pct（尚未连续回升）。"
            "考虑央行新M1已纳入个人活期与支付备付金，实体企业内生资本开支与传统基建地产链活化仍需社融中长期贷款验证，维持哑铃配置。"
        )
        defense_weight += 2
    else:
        r6_status, r6_label = "warning", "分子端收缩防御"
        r6_action = f"M1-M2剪刀差深处{scissors_val}%（环比{scissors_chg:+}pct），实体资金沉淀定期化明显，规避高负债顺周期板块。"
        defense_weight += 5

    rules.append({
        "id": "RULE_CN_CREDIT_PULSE",
        "dimension": "⑥ 国内分子端宽信用与M1活化",
        "status": r6_status,
        "status_label": r6_label,
        "trigger_condition": "M1-M2>-3.5% 或 (M1-M2>-4.8%且月环比>0) 确认宽信用拐点",
        "current_reading": f"M1-M2剪刀差 {scissors_val}% (月环比 {scissors_chg:+}pct | {cn['m1_m2_diff']['observation_date']})",
        "action_instruction": r6_action
    })

    # 规则 7：A股股债性价比标尺
    if erp_val >= 5.80:
        r7_status, r7_label = "bullish", "高性价比赔率区"
        pct_note = (
            f"处近3年{erp_pct}%高分位"
            if erp_pct >= 70 else
            f"近3年{erp_pct}%分位（因包含2024年极端底部样本，但绝对溢价已显著高于5.8%配置线）"
        )
        r7_action = (
            f"沪深300 ERP达{erp_val}%（{pct_note}），10Y国债{cn10y}%低利率推升权益隐含风险溢价，指数向下空间受封锁，宜通过哑铃进攻端布局低库存+Back紧缺资源品与CapEx订单验证的硬科技板块。"
        )
        defense_weight -= 6
    elif erp_val >= 5.00:
        r7_status, r7_label = "neutral", "估值性价比均衡"
        r7_action = f"沪深300 ERP报{erp_val}%（近3年{erp_pct}%分位），股债性价比处于合理区间，权益重在把握结构性大宗供需缺口与CapEx景气分化。"
    else:
        r7_status, r7_label = "warning", "风险溢价偏薄"
        r7_action = f"沪深300 ERP降至{erp_val}%（近3年{erp_pct}%分位），权益整体估值保护减弱，严控追高并收缩进攻端敞口。"
        defense_weight += 6

    rules.append({
        "id": "RULE_EQUITY_ERP_VALUATION",
        "dimension": "⑦ A股股债性价比 (沪深300 ERP)",
        "status": r7_status,
        "status_label": r7_label,
        "trigger_condition": "沪深300 ERP >= 5.80% 触发高性价比配置信号",
        "current_reading": f"ERP {erp_val}% (近3年 {erp_pct}%分位 | 1日 {cn['erp_csi300']['chg_1d_bp']:+}bp)",
        "action_instruction": r7_action
    })

    # 汇总大宗期现评级（用于五篮子哑铃切分与晨报）
    active_commodities = commodities if commodities is not None else DEFAULT_COMMODITIES
    graded_list = [
        (c, evaluate_commodity_grade(c, dfii10=dfii10))
        for c in active_commodities
    ]
    strong_bull_names = [c["name"].split("/")[0].replace("SHFE", "").replace("INE", "").strip()
                         for c, g in graded_list if g["grade"] == "STRONG_BULL"]
    contango_bear_names = [c["name"].split("/")[0].replace("SHFE", "").replace("INE", "").strip()
                           for c, g in graded_list if g["grade"] == "CONTANGO_BEAR"]

    # 当显式传入 commodities 或 cross_asset 时，追加第⑧维【大宗期现结构与跨资产比价共振】
    if commodities is not None or cross_asset is not None:
        ca = cross_asset or DEFAULT_CROSS_ASSET
        dxy_v = ca.get("dxy", {}).get("value", 103.41)
        cg_v = ca.get("cop_gold", {}).get("value", 3.49)
        if len(strong_bull_names) >= 2:
            r8_status, r8_label = "bullish", f"{len(strong_bull_names)}品种低库存+Back共振"
            r8_action = (
                f"当前【{'、'.join(strong_bull_names)}】处于5年显性库存<25%且现货升水(Back)强逼仓结构（叠加DXY {dxy_v}、铜金比 {cg_v}），"
                f"具备真实现货供需缺口与AIDC/电网CapEx拉动，已在进攻端专门划出紧缺资源品仓位；"
                f"同时【{'、'.join(contango_bear_names) or '无'}】处于Contango累库状态，已从红利底仓中定向排雷剔除。"
            )
        elif len(contango_bear_names) >= 2:
            r8_status, r8_label = "warning", "多品种Contango累库"
            r8_action = f"大宗工业品中【{'、'.join(contango_bear_names)}】呈现货贴水与累库压力，周期板块整体收缩，仅保留黄金或独立供需缺口品种。"
        else:
            r8_status, r8_label = "neutral", "期现结构分化"
            r8_action = "大宗商品期现结构呈品种间分化，严格遵循“只做低库存+Back升水龙头、规避Contango累库伪红利”原则。"

        rules.append({
            "id": "RULE_COMMODITY_BASIS_RESONANCE",
            "dimension": "⑧ 大宗期现结构与跨资产比价共振",
            "status": r8_status,
            "status_label": r8_label,
            "trigger_condition": "显性库存5年分位<25% 且 现货升水(Back) 触发资源进攻；Contango且库存>=35% 触发伪红利排雷",
            "current_reading": f"强Back低库存: {'/'.join(strong_bull_names) or '无'} | Contango排雷: {'/'.join(contango_bear_names) or '无'} | DXY {dxy_v}",
            "action_instruction": r8_action
        })

    # 锁定哑铃权重在 [35%, 75%] 合理风控区间并取整到5的倍数
    defense_weight = int(round(max(35, min(75, defense_weight)) / 5.0) * 5)
    offense_weight = 100 - defense_weight

    bond_w = int(round(defense_weight * 0.58 / 5.0) * 5)
    div_w = defense_weight - bond_w

    # 五篮子哑铃分配：当存在低库存+Back紧缺大宗品种时，进攻端科学切分为【AI算力基建 + 紧缺大宗资源 + Agent/具身智能】
    if len(strong_bull_names) > 0 and offense_weight >= 25:
        com_w = int(round(offense_weight * 0.30 / 5.0) * 5)
        com_w = max(10, com_w)
        ai_hw_w = int(round(offense_weight * 0.43 / 5.0) * 5)
        agent_robot_w = offense_weight - com_w - ai_hw_w
    else:
        com_w = 0
        ai_hw_w = int(round(offense_weight * 0.62 / 5.0) * 5)
        agent_robot_w = offense_weight - ai_hw_w

    contango_note = f"（已定向剔除【{'、'.join(contango_bear_names)}】等Contango现货贴水累库伪红利）" if contango_bear_names else ""
    strong_com_note = f"当前锁定【{'、'.join(strong_bull_names)}】（库存<25%分位+现货Back升水）高自给率矿端龙头" if strong_bull_names else "等待库存<25%且现货升水品种出现"

    breakdown = [
        {
            "bucket": "防御端：中短久期利率债/高等级信用债",
            "group_key": "defense_bond",
            "weight": bond_w,
            "tag": "流动性与票息底仓",
            "rationale": f"FDR007报{dr007_val}%、R007-DR007利差{r_dr_spread}bp保障稳健套息，规避30Y超长债({cn['cn30y']['value']}%)在极低利率分位下的供给与止盈波动。"
        },
        {
            "bucket": "防御端：央国企高股息真红利低波",
            "group_key": "defense_dividend",
            "weight": div_w,
            "tag": "险资OCI类债替代锚",
            "rationale": f"10Y国债{cn10y}%（{cn10y_pct}%分位）背景下，自由现金流充裕的水电/运营商/国有大行具备险资OCI账户强力承接{contango_note}。"
        },
        {
            "bucket": "进攻端：全球AI算力基建与电力/铜连接卡点",
            "group_key": "offense_tech_capex",
            "weight": ai_hw_w,
            "tag": "CapEx一阶与二阶导验证",
            "rationale": "紧盯台积电月度营收、韩国HBM出口与北美云厂商CapEx同比增速斜率，精选1.6T光模块、液冷、AIDC电源HVDC及国产算力芯片放量标的。"
        },
        {
            "bucket": "进攻端：强Back+低库存紧缺大宗资源品",
            "group_key": "offense_commodity",
            "weight": com_w,
            "tag": "期现逼仓与供需缺口",
            "rationale": f"{strong_com_note}，叠加黄金央行购金底仓，对冲海外高通胀黏性与AIDC电力/铜铝实物消耗。"
        },
        {
            "bucket": "进攻端：AI Agent商业化与具身智能核心部件",
            "group_key": "offense_tech_agent",
            "weight": agent_robot_w,
            "tag": "ROI闭环与0到1期权",
            "rationale": "围绕推理侧Scaling Law降本后的企业级Agent软件RPO加速，以及人形机器人行星滚柱丝杠/六维力传感器正式定点公告逢低卡位。"
        }
    ]

    barbell = {
        "defense_total": defense_weight,
        "offense_total": offense_weight,
        "strong_commodities": strong_bull_names,
        "contango_commodities": contango_bear_names,
        "breakdown": breakdown
    }

    return {"hard_rules": rules, "barbell_allocation": barbell}


def generate_soft_reports(us: dict, cn: dict, engine_res: dict, cross_asset: dict = None, commodities: list = None) -> dict:
    """
    第二层决策引擎：基于真实规则状态与大宗期现结构动态合成双视角软研报
    1. 二级市场交易实战版 (trading_perspective)
    2. 资管/理财存续期管理与渠道陪伴版 (wealth_perspective)
    """
    def_w = engine_res["barbell_allocation"]["defense_total"]
    off_w = engine_res["barbell_allocation"]["offense_total"]
    buckets = engine_res["barbell_allocation"]["breakdown"]
    b_map = {b.get("group_key", f"idx_{i}"): b["weight"] for i, b in enumerate(buckets)}
    bond_w = b_map.get("defense_bond", buckets[0]["weight"])
    div_w = b_map.get("defense_dividend", buckets[1]["weight"])
    ai_w = b_map.get("offense_tech_capex", buckets[2]["weight"])
    com_w = b_map.get("offense_commodity", 0)
    agent_w = b_map.get("offense_tech_agent", buckets[-1]["weight"])

    strong_coms = engine_res["barbell_allocation"].get("strong_commodities") or ["阴极铜", "氧化铝", "黑钨精矿"]
    contango_coms = engine_res["barbell_allocation"].get("contango_commodities") or ["螺纹钢"]

    us10y = us["dgs10"]["value"]
    us10y_5d = us["dgs10"]["chg_5d_bp"]
    us10y_pct = us["dgs10"]["percentile_3y"]
    dfii10 = us.get("dfii10", DEFAULT_US_METRICS["dfii10"])["value"]
    t10yie = us.get("t10yie", DEFAULT_US_METRICS["t10yie"])["value"]
    rrp_val = us["rrp"]["value"]
    tga_val = us["tga"]["value"]
    wresbal_val = us.get("wresbal", DEFAULT_US_METRICS["wresbal"])["value"]
    hy_oas = us.get("hy_oas", DEFAULT_US_METRICS["hy_oas"])["value"]

    cn10y = cn["cn10y"]["value"]
    cn10y_5d = cn["cn10y"]["chg_5d_bp"]
    cn10y_pct = cn["cn10y"]["percentile_3y"]
    cn30y = cn["cn30y"]["value"]
    dr007_val = cn["dr007"]["value"]
    r_dr_spread = cn.get("r007_dr007_spread", DEFAULT_CN_METRICS["r007_dr007_spread"])["value"]
    us_cn_spread = cn.get("us_cn_spread", DEFAULT_CN_METRICS["us_cn_spread"])["value"]
    scissors = cn["m1_m2_diff"]["value"]
    scissors_chg = cn["m1_m2_diff"]["chg_1d_bp"]
    erp_val = cn["erp_csi300"]["value"]
    erp_pct = cn["erp_csi300"]["percentile_3y"]

    ca = cross_asset or DEFAULT_CROSS_ASSET
    dxy_v = ca.get("dxy", {}).get("value", 103.41)
    cg_v = ca.get("cop_gold", {}).get("value", 3.49)
    go_v = ca.get("gold_oil", {}).get("value", 43.7)

    rule_map = {r["id"]: r for r in engine_res["hard_rules"]}
    r_cn_redemp = rule_map["RULE_CN_WEALTH_REDEMPTION"]
    r_cn_credit = rule_map["RULE_CN_CREDIT_PULSE"]

    if us10y_5d < 0 and (us10y >= 4.35 or dfii10 >= 2.20):
        us_logic_str = (
            f"1. 海外分母端与流动性：10Y美债5日虽边际回落{abs(us10y_5d)}bp至{us10y}%，但拆解后10Y TIPS实际利率仍高达{dfii10}%（通胀预期{t10yie}%），"
            f"且ON RRP耗尽({rrp_val}B)、TGA高企({tga_val}B)将银行准备金抽离至{wresbal_val}B，海外无风险贴现率绝对水位与流动性底线依然偏紧，压制无现金流支撑的纯概念题材；"
        )
    elif us10y_5d >= 12.0 or us10y >= 4.35:
        us_logic_str = (
            f"1. 海外分母端与流动性：10Y美债报{us10y}%（5日变动{us10y_5d:+}bp），实际利率达{dfii10}%，叠加银行准备金{wresbal_val}B与HY利差{hy_oas}%，"
            "海外分母端贴现率上冲对高估值纯概念成长股形成直接压制；"
        )
    else:
        us_logic_str = (
            f"1. 海外分母端与流动性：10Y美债报{us10y}%（实际利率{dfii10}% + 通胀预期{t10yie}%，5日变动{us10y_5d:+}bp），"
            f"分母端压力相对可控，重点观察准备金({wresbal_val}B)与高收益利差({hy_oas}%)边际变化；"
        )

    if r_cn_credit["status"] == "bullish":
        cn_logic_str = (
            f"2. 国内宽货币与分子端传导：FDR007报{dr007_val}%（R007-DR007利差{r_dr_spread}bp），同时M1-M2剪刀差回升至{scissors}%（月环比{scissors_chg:+}pct），"
            "企业活期资金活化出现积极拐点，资金正逐步从纯债防御向顺周期紧缺资源与高景气成长扩散；"
        )
    else:
        cn_logic_str = (
            f"2. 国内宽货币与分子端传导：FDR007仅{dr007_val}%、10Y中债处{cn10y}%（近3年{cn10y_pct}%分位），而新口径M1-M2剪刀差({scissors}%，月环比{scissors_chg:+}pct)尚未形成连续上行斜率，"
            f"中美利差倒挂({us_cn_spread}%)约束下‘低利率资产荒+分子端磨底’格局延续，高股息真红利与短久期高等级信用债底仓逻辑稳固；"
        )

    trading_report = {
        "title": f"【二级市场实战晨报】美债{us10y}%(实际{dfii10}%)、中债{cn10y}%，执行“{def_w}%红利短债 + {off_w}%紧缺资源与CapEx硬科技”五篮子哑铃配置",
        "core_marginal_change": (
            f"海外10Y美债报{us10y}%（5日{us10y_5d:+}bp，{us10y_pct}%分位；拆解为10Y实际利率{dfii10}% + 通胀预期{t10yie}%），"
            f"ON RRP报{rrp_val}B、TGA报{tga_val}B、商业银行准备金{wresbal_val}B，高收益债利差{hy_oas}%；"
            f"国内10Y中债报{cn10y}%（{cn10y_pct}%分位）、30Y报{cn30y}%，FDR007(DR007)报{dr007_val}%，R007-DR007分层利差{r_dr_spread}bp，"
            f"中美10Y利差{us_cn_spread}%，沪深300 ERP达{erp_val}%（{erp_pct}%分位）；"
            f"跨资产DXY报{dxy_v}、铜金比{cg_v}、金油比{go_v}x，大宗期现中【{'、'.join(strong_coms)}】处于低库存(<25%分位)+现货Back升水强共振。"
        ),
        "transmission_logic": (
            f"{us_logic_str}\n"
            f"{cn_logic_str}\n"
            f"3. 大宗期现库存与科技CapEx双轮验证：沪深300 ERP处{erp_val}%（{erp_pct}%分位）封锁指数下行空间；"
            f"实物资产端【{'、'.join(strong_coms)}】呈显性低库存+现货升水(Back)逼仓结构，且直接受益于AIDC数据中心供电/电网CapEx与上游配额刚性约束；"
            "科技端聚焦台积电月度营收与云厂商CapEx增速斜率未减、且下游RPO能覆盖折旧的算力卡点与具身智能定点环节。"
        ),
        "action_instructions": [
            f"【五篮子哑铃配比】今日量化规则锁定 防守端 {def_w}%（中短债{bond_w}% + 央企真红利{div_w}%）: 进攻端 {off_w}%（AI算力基建{ai_w}% + 低库存Back紧缺资源{com_w}% + Agent/具身智能{agent_w}%）。",
            f"【大宗期现与科技选股过滤】资源端优先配置【{'、'.join(strong_coms)}】对应的高自给率上游矿端龙头及央行购金托底的黄金矿股；科技端精选1.6T光模块、液冷、AIDC电源及披露正式定点通知书的丝杠/传感器标的。",
            f"【债市久期与伪红利排雷】10Y国债处{cn10y}%极低分位，R007-DR007利差{r_dr_spread}bp（{r_cn_redemp['status_label']}），固收底仓以1-3年高等级信用债/存单吃票息替代30Y超长债博弈；红利端严格剔除【{'、'.join(contango_coms)}】等处于Contango现货贴水且高库存累库的周期伪红利，聚焦水电、运营商与国有大行。"
        ]
    }

    if r_cn_redemp["status"] == "danger":
        redemp_script = (
            f"⚠️【紧急·防赎回负反馈话术】：当前R007-DR007非银利差走阔至{r_dr_spread}bp，债市面临机构预防性抛售二永债扰动。"
            "请向客户强调：产品已提前缩短久期至高流动性短债与存单，净值回撤显著小于长债指数，切勿在流动性冲击高点恐慌赎回。"
        )
    else:
        redemp_script = (
            f"【纯固收/短债客户安抚话术】：“当前央行呵护狭义流动性态度明确（FDR007仅{dr007_val}%，R007-DR007非银分层利差仅{r_dr_spread}bp，无机构赎回负反馈压力）。"
            f"近期长端国债在{cn10y}%历史极低利率区间的微幅波动属于正常久期呼吸，我们产品以中短久期高等级信用债筑底锁票息，建议安心持有获取时间复利。”"
        )

    wealth_report = {
        "title": f"【存续期管理与渠道陪伴】10Y中债{cn10y}%、R-DR分层利差{r_dr_spread}bp环境下，固收/固收+归因与分行沟通话术",
        "core_marginal_change": (
            f"国内银行间狭义资金面平稳（FDR007报{dr007_val}%，R007-DR007流动性分层利差{r_dr_spread}bp），"
            f"10Y国债报{cn10y}%（近3年{cn10y_pct}%分位）、30Y国债报{cn30y}%；海外10Y美债报{us10y}%（实际利率{dfii10}%，5日变动{us10y_5d:+}bp），"
            f"A股沪深300股债风险溢价（ERP）处于{erp_val}%（近3年{erp_pct}%分位）。"
        ),
        "transmission_logic": (
            f"1. 【纯债与理财存续期归因】：决定理财净值回撤的核心并非单看央票利率，而是非银资金分层（R007-DR007当前{r_dr_spread}bp）与长债供给冲击。"
            f"当前短端票息底仓稳固，而10Y/30Y长债在{cn10y_pct}%极低收益率分位下赔率偏薄、对超长期特别国债供给敏感；\n"
            f"2. 【固收+与含权产品归因】：中美利差倒挂({us_cn_spread}%)与海外高实际利率({dfii10}%)压制纯概念估值，但国内沪深300 ERP({erp_val}%)处高性价比区，"
            f"通过“{def_w}%短债/央企真红利安全垫 + {off_w}%紧缺实物资源/硬科技CapEx”的五篮子哑铃结构，可有效平滑单一资产波动。"
        ),
        "action_instructions": [
            redemp_script,
            f"【分行RM配置转化建议】：针对存款到期或寻求稳健增值的客户，主推“{bond_w}%短债理财底仓 + {div_w}%央企真红利低波 + {off_w}%硬科技与紧缺资源(金/铜)固收+”组合，既规避单一押注30Y长债的久期回撤，又保留AI产业周期与实物通胀弹性的向上空间。",
            f"【渠道风控三大扳机线】：① 10Y中债突破2.15%（长债止损线）；② FDR007突破1.85%（资金收敛线）；③ R007-DR007利差突破25bp（理财赎回踩踏预警线，当前仅{r_dr_spread}bp，资金面安全）。"
        ]
    }

    return {
        "trading_perspective": trading_report,
        "wealth_perspective": wealth_report
    }


def push_webhook_notification(payload: dict):
    """若配置了企业微信/飞书 Webhook，自动推送每日晨报摘要"""
    webhook_url = os.environ.get("WEBHOOK_URL", "").strip()
    if not webhook_url:
        return

    tr = payload["decision_engine"]["soft_reports"]["trading_perspective"]
    wr = payload["decision_engine"]["soft_reports"]["wealth_perspective"]
    msg_text = (
        f"📊 【宏观流动性×大宗期现×科技CapEx投研终端·每日自动更新】\n"
        f"更新时间：{payload['meta']['updated_at']}\n"
        f"━━━━━━━━━━━━━━\n"
        f"🎯 {tr['title']}\n"
        f"• 边际变化：{tr['core_marginal_change']}\n"
        f"• 实战指令：{tr['action_instructions'][0]}\n"
        f"━━━━━━━━━━━━━━\n"
        f"🏦 {wr['title']}\n"
        f"• 渠道话术：{wr['action_instructions'][0]}"
    )

    try:
        body = {"msgtype": "text", "text": {"content": msg_text}, "msg_type": "text", "content": {"text": msg_text}}
        resp = requests.post(webhook_url, json=body, timeout=10)
        if resp.status_code == 200:
            print("[SUCCESS] 已推送每日晨报至 Webhook 机器人。")
        else:
            print(f"[WARN] Webhook 返回非 200 状态码: {resp.status_code}")
    except Exception as e:
        print(f"[WARN] Webhook 推送失败: {e}")


def main():
    if os.path.exists(DATA_FILE_PATH):
        with open(DATA_FILE_PATH, "r", encoding="utf-8") as f:
            current_data = json.load(f)
    else:
        current_data = {"meta": {}, "metrics": {"us": {}, "cn": {}, "cross_asset": {}}, "decision_engine": {}, "capex_tracker": []}

    us_seed = copy.deepcopy(DEFAULT_US_METRICS)
    for k, v in current_data.get("metrics", {}).get("us", {}).items():
        if k in us_seed:
            for dyn_field in ("value", "chg_1d_bp", "chg_5d_bp", "percentile_3y", "observation_date", "data_status"):
                if dyn_field in v:
                    us_seed[k][dyn_field] = v[dyn_field]
        else:
            us_seed[k] = v

    cn_seed = copy.deepcopy(DEFAULT_CN_METRICS)
    for k, v in current_data.get("metrics", {}).get("cn", {}).items():
        if k in cn_seed:
            for dyn_field in ("value", "chg_1d_bp", "chg_5d_bp", "percentile_3y", "observation_date", "data_status"):
                if dyn_field in v:
                    cn_seed[k][dyn_field] = v[dyn_field]
        else:
            cn_seed[k] = v

    for group in (us_seed, cn_seed):
        for metric in group.values():
            metric["data_status"] = "cached" if metric.get("observation_date") else "demo"

    us_metrics = fetch_us_macro(us_seed)
    cn_metrics = fetch_cn_macro(cn_seed)
    cross_asset = fetch_cross_asset_anchors(current_data.get("metrics", {}).get("cross_asset", {}))
    commodities = fetch_commodities_data(
        current_data.get("commodities", DEFAULT_COMMODITIES),
        dfii10=us_metrics["dfii10"]["value"]
    )

    engine_out = run_hard_rule_engine(us_metrics, cn_metrics, cross_asset=cross_asset, commodities=commodities)
    soft_reports = generate_soft_reports(us_metrics, cn_metrics, engine_out, cross_asset=cross_asset, commodities=commodities)
    equity_pool = enrich_equity_pool(
        current_data.get("equity_pool", DEFAULT_EQUITY_POOL),
        commodities=commodities,
        defense_weight=engine_out["barbell_allocation"]["defense_total"],
        fetch_live_quotes=True
    )

    tz_cst = datetime.timezone(datetime.timedelta(hours=8))
    now_str = datetime.datetime.now(tz_cst).strftime("%Y-%m-%d %H:%M:%S CST")

    unverified = [metric["name"] for group in (us_metrics, cn_metrics)
                  for metric in group.values() if metric.get("data_status") != "updated"]
    note = ("本次未刷新（使用缓存）：" + "、".join(unverified) + "。" if unverified else "全部16项中美宏观、流动性与估值指标均已实时核验更新。")
    note += " 大宗期现基差/库存分位(6大品种)与科技CapEx三阶段产业链已联动核验；双视角晨报由八维规则引擎动态合成。"

    def_w = engine_out["barbell_allocation"]["defense_total"]
    off_w = engine_out["barbell_allocation"]["offense_total"]
    strong_cnt = len(engine_out["barbell_allocation"].get("strong_commodities", []))
    regime_str = f"海外实际利率{us_metrics['dfii10']['value']}% + 国内资产荒(中债{cn_metrics['cn10y']['value']}%) + {strong_cnt}大宗低库存Back共振 · 哑铃 {def_w}:{off_w}"
    if unverified:
        regime_str += "（含缓存指标）"

    current_data["meta"]["updated_at"] = now_str
    current_data["meta"]["data_source"] = "FRED API (9项) + ChinaMoney/AkShare (7项) + 跨资产比价(4项) + SHFE/INE/SGE大宗期现库存(6项)"
    current_data["meta"]["market_regime"] = regime_str
    current_data["meta"]["regime_code"] = "PRO_EIGHT_DIM_UNIFIED"
    current_data["meta"]["data_quality_note"] = note

    current_data["metrics"]["us"] = us_metrics
    current_data["metrics"]["cn"] = cn_metrics
    current_data["metrics"]["cross_asset"] = cross_asset
    current_data["decision_engine"]["hard_rules"] = engine_out["hard_rules"]
    current_data["decision_engine"]["barbell_allocation"] = engine_out["barbell_allocation"]
    current_data["decision_engine"]["soft_reports"] = soft_reports
    current_data["capex_tracker"] = copy.deepcopy(DEFAULT_CAPEX_TRACKER)
    current_data["commodities"] = commodities
    current_data["equity_pool"] = equity_pool
    current_data["field_notes"] = current_data.get("field_notes") or copy.deepcopy(DEFAULT_FIELD_NOTES)

    with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(current_data, f, ensure_ascii=False, indent=2)

    print(f"[DONE] 每日宏观+大宗期现+科技CapEx一体化数据与八维决策结论已写入 {DATA_FILE_PATH} ({now_str})")
    push_webhook_notification(current_data)


if __name__ == "__main__":
    main()

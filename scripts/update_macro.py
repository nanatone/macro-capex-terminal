#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宏观流动性与科技CapEx一手数据自动抓取及双层决策引擎 (Pro 实战与理财渠道终端版)
==========================================================================
架构说明：
1. 数据采集层 (ETL)：
   - 美国宏观 (FRED API)：
     DGS10 (10Y美债名义利率)、DFII10 (10Y TIPS实际利率)、T10YIE (10Y盈亏平衡通胀率)、
     T10Y2Y (10Y-2Y期限利差)、EFFR (联邦基金有效利率)、RRPONTSYD (ON RRP隔夜逆回购)、
     WTREGEN (财政部TGA账户)、WRESBAL (美联储商业银行准备金)、BAMLH0A0HYM2 (美债高收益利差 HY OAS)
   - 中国宏观与跨市场 (AkShare / 中国外汇交易中心 / 东方财富 / 乐咕)：
     CN10Y/CN30Y (中债10Y/30Y国债收益率)、FDR007 (中国外汇交易中心银银间7天回购定盘利率/DR007基准)、
     R007-DR007 (FR007-FDR007 银行间非银流动性分层利差)、CN10Y-US10Y (中美10Y国债利差)、
     M1-M2 同比剪刀差 (央行新口径货币供应量)、CSI300_ERP (沪深300股债风险溢价)
2. 第一层决策：硬逻辑量化阈值触发器 (Rule-Based Engine)
   - 覆盖七大实战传导维度（海外实际利率/通胀拆解、美元准备金临界水位、中美利差与汇率约束、
     国内低利率资产荒、理财/债基预防性赎回负反馈预警、新口径M1-M2宽信用斜率、A股ERP历史分位）
   - 动态推导哑铃型策略 (Barbell Strategy) 防守端与进攻端最优配比（35%~75%，5%步长）
3. 第二层决策：条件驱动动态双视角晨报引擎 (Dynamic Soft Report Engine)
   - 严格基于当日真实边际变化与规则触发状态动态组装【二级市场交易实战版】与【存续期管理与渠道陪伴版】
4. 消息推送 (可选)：
   - 若配置了 WEBHOOK_URL，支持向企业微信 / 飞书群推送每日双视角摘要。
"""

import os
import copy
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

DEFAULT_US_METRICS = {
    "dgs10": {
        "name": "10Y 美债名义收益率",
        "code": "DGS10",
        "value": 4.94,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -4.0,
        "chg_5d_bp": -6.0,
        "percentile_3y": 95,
        "threshold_warn": 4.35,
        "role": "DCF分母总锚：名义利率 = 实际利率(DFII10) + 通胀预期(T10YIE)",
        "url": "https://fred.stlouisfed.org/series/DGS10",
        "data_status": "cached",
        "observation_date": "2026-10-30"
    },
    "dfii10": {
        "name": "10Y TIPS 实际利率",
        "code": "DFII10",
        "value": 2.60,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -3.0,
        "chg_5d_bp": -4.0,
        "percentile_3y": 96,
        "threshold_warn": 2.20,
        "role": "DCF真实分母端：剔除通胀后的全球科技成长股真实无风险贴现率",
        "url": "https://fred.stlouisfed.org/series/DFII10",
        "data_status": "cached",
        "observation_date": "2026-10-30"
    },
    "t10yie": {
        "name": "10Y 盈亏平衡通胀预期",
        "code": "T10YIE",
        "value": 2.29,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -5.0,
        "chg_5d_bp": -4.0,
        "percentile_3y": 44,
        "threshold_warn": 2.45,
        "role": "DCF分子端映射：隐含通胀与增长预期（急跌<1.9%预示衰退交易）",
        "url": "https://fred.stlouisfed.org/series/T10YIE",
        "data_status": "cached",
        "observation_date": "2026-11-02"
    },
    "t10y2y": {
        "name": "10Y-2Y 美债期限利差",
        "code": "T10Y2Y",
        "value": 0.32,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -6.0,
        "chg_5d_bp": 0.0,
        "percentile_3y": 46,
        "threshold_warn": 0.50,
        "role": "收益率曲线形态与长债期限溢价(Term Premium)周期",
        "url": "https://fred.stlouisfed.org/series/T10Y2Y",
        "data_status": "cached",
        "observation_date": "2026-11-02"
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
        "role": "美联储短端政策利率基准与货币市场资金成本锚",
        "url": "https://fred.stlouisfed.org/series/EFFR",
        "data_status": "cached",
        "observation_date": "2026-10-30"
    },
    "rrp": {
        "name": "美联储隔夜逆回购 (ON RRP)",
        "code": "RRPONTSYD",
        "value": 5.09,
        "unit": "十亿美元",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": 0.7,
        "chg_5d_bp": 4.9,
        "percentile_3y": 29,
        "threshold_warn": 200.0,
        "role": "美元非银超额流动性第一道缓冲池（耗尽后QT直接抽离银行准备金）",
        "url": "https://fred.stlouisfed.org/series/RRPONTSYD",
        "data_status": "cached",
        "observation_date": "2026-11-02"
    },
    "tga": {
        "name": "美国财政部一般账户 (TGA)",
        "code": "WTREGEN",
        "value": 999.73,
        "unit": "十亿美元",
        "freq": "weekly",
        "window_label": "近3年历史分位数(周频)",
        "chg_1d_bp": 47.7,
        "chg_5d_bp": 22.6,
        "percentile_3y": 99,
        "threshold_warn": 800.0,
        "role": "财政发债抽水(>800B显著收紧市场准备金)与财政支出投放账户",
        "url": "https://fred.stlouisfed.org/series/WTREGEN",
        "data_status": "cached",
        "observation_date": "2026-10-28"
    },
    "wresbal": {
        "name": "美联储商业银行准备金余额",
        "code": "WRESBAL",
        "value": 2893.77,
        "unit": "十亿美元",
        "freq": "weekly",
        "window_label": "近3年历史分位数(周频)",
        "chg_1d_bp": -68.8,
        "chg_5d_bp": -185.4,
        "percentile_3y": 2,
        "threshold_warn": 3000.0,
        "role": "美元流动性最终底线（跌破3000B触发华尔街回购市场资金紧缺预警）",
        "url": "https://fred.stlouisfed.org/series/WRESBAL",
        "data_status": "cached",
        "observation_date": "2026-10-28"
    },
    "hy_oas": {
        "name": "美国高收益债利差 (HY OAS)",
        "code": "BAMLH0A0HYM2",
        "value": 3.44,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": 18.0,
        "chg_5d_bp": 29.0,
        "percentile_3y": 65,
        "threshold_warn": 4.00,
        "role": "跨市场信用风险溢价与全球套息交易(Carry Trade)去杠杆温度计",
        "url": "https://fred.stlouisfed.org/series/BAMLH0A0HYM2",
        "data_status": "cached",
        "observation_date": "2026-10-30"
    }
}

DEFAULT_CN_METRICS = {
    "cn10y": {
        "name": "10Y 中债国债收益率",
        "code": "CN10Y",
        "value": 1.62,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -1.3,
        "chg_5d_bp": -0.5,
        "percentile_3y": 1,
        "threshold_warn": 2.15,
        "role": "DCF分母端：国内无风险利率核心锚与低利率资产荒温度计",
        "url": "https://yield.chinabond.com.cn/",
        "data_status": "cached",
        "observation_date": "2026-11-03"
    },
    "cn30y": {
        "name": "30Y 超长国债收益率",
        "code": "CN30Y",
        "value": 2.05,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": -1.5,
        "chg_5d_bp": 1.0,
        "percentile_3y": 23,
        "threshold_warn": 2.35,
        "role": "险资长久期配置锚与超长期特别国债供给冲击敏感度指标",
        "url": "https://yield.chinabond.com.cn/",
        "data_status": "cached",
        "observation_date": "2026-11-03"
    },
    "dr007": {
        "name": "银行间7天回购定盘利率 (FDR007/DR007)",
        "code": "FDR007",
        "value": 1.35,
        "unit": "%",
        "freq": "daily",
        "window_label": "近1年历史分位数(外汇交易中心)",
        "chg_1d_bp": -1.0,
        "chg_5d_bp": 2.0,
        "percentile_3y": 14,
        "threshold_warn": 1.80,
        "role": "中国外汇交易中心存款类机构7天质押式回购定盘基准（狭义资金面核心锚）",
        "url": "https://www.chinamoney.com.cn/chinese/bkfrr/",
        "data_status": "cached",
        "observation_date": "2026-11-03"
    },
    "r007_dr007_spread": {
        "name": "R007 - DR007 流动性分层利差",
        "code": "FR007-FDR007",
        "value": 5.0,
        "unit": "bp",
        "freq": "daily",
        "window_label": "近1年历史分位数(外汇交易中心)",
        "chg_1d_bp": 3.0,
        "chg_5d_bp": 0.3,
        "percentile_3y": 35,
        "threshold_warn": 20.0,
        "role": "非银机构/理财子资金松紧与债基/理财预防性赎回负反馈领先预警器",
        "url": "https://www.chinamoney.com.cn/chinese/bkfrr/",
        "data_status": "cached",
        "observation_date": "2026-11-03"
    },
    "us_cn_spread": {
        "name": "中美 10Y 国债利差 (中-美)",
        "code": "CN10Y-US10Y",
        "value": -3.28,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": 1.4,
        "chg_5d_bp": 4.2,
        "percentile_3y": 1,
        "threshold_warn": -2.20,
        "role": "跨境资本流动、人民币汇率稳汇率约束与港股离岸市场分母端锚",
        "url": "https://yield.chinabond.com.cn/",
        "data_status": "cached",
        "observation_date": "2026-11-02"
    },
    "m1_m2_diff": {
        "name": "M1 - M2 同比剪刀差 (新口径)",
        "code": "M1-M2",
        "value": -4.0,
        "unit": "%",
        "freq": "monthly",
        "window_label": "近3年(36月)分位数",
        "chg_1d_bp": -0.6,
        "chg_5d_bp": -0.6,
        "percentile_3y": 58,
        "threshold_warn": -3.5,
        "role": "DCF分子端：实体企业活期存款活化与宽信用向顺周期传导成色",
        "url": "http://www.pbc.gov.cn/diaochatongjisi/116219/index.html",
        "data_status": "cached",
        "observation_date": "2026年09月份"
    },
    "erp_csi300": {
        "name": "沪深300 股债风险溢价 (ERP)",
        "code": "CSI300_ERP",
        "value": 6.25,
        "unit": "%",
        "freq": "daily",
        "window_label": "近3年历史分位数",
        "chg_1d_bp": 2.6,
        "chg_5d_bp": -28.8,
        "percentile_3y": 78,
        "threshold_warn": 5.80,
        "role": "A股股债性价比标尺（>=5.8%且处高分位代表权益向下空间封锁）",
        "url": "https://www.csindex.com.cn/",
        "data_status": "cached",
        "observation_date": "2026-11-03"
    }
}

DEFAULT_CAPEX_TRACKER = [
    {
        "stage": "第一阶段：算力与硬件基建 (先导指标与二阶导验证)",
        "status": "高景气兑现期 · 盯紧增速斜率",
        "badge_color": "emerald",
        "chain_nodes": "台积电CoWoS先进封装 ➔ HBM3e/HBM4存储 ➔ 800G/1.6T光模块与CPO ➔ AIDC液冷(CDU)与HVDC供电",
        "lead_indicators": "① 每月10日：台积电(TSMC)月度合并营收同比/环比；② 每月1/11/21日：韩国关税厅前10/20日半导体与HBM出口额；③ 北美四大云厂商 CapEx / 经营性现金流(OCF) 比例。",
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
        "verification_metric": "【一阶导+二阶导】北美四大云厂商(MSFT/GOOG/META/AMZN)季度CapEx同比 > 35%，且下季指引环比增速不降，云业务营收增量能覆盖新增算力折旧摊销。",
        "red_flag": "【二阶导见顶预警】云厂商CapEx绝对值虽创新高但同比增速斜率连续放缓（触发硬件杀估值），或光模块/服务器ODM厂商存货周转天数攀升、出现重复下单(Double Ordering)砍单。"
    },
    {
        "stage": "第二阶段：推理期扩展与 Agent 商业化 (ROI 闭环期)",
        "status": "分化验证期 · 考核RPO与毛利",
        "badge_color": "blue",
        "chain_nodes": "Reasoning推理期算力扩展 ➔ 端侧轻量化蒸馏模型 ➔ 企业级AI Agent工作流 ➔ 垂直行业SaaS增收(ARR)",
        "lead_indicators": "① 企业软件财报 RPO(剩余履约义务)增速 vs 营收增速剪刀差；② 主流算力租赁平台 GPU 时租价格(GPU Hour Rate)走势；③ 企业级 Copilot/Agent 净金额留存率(NDR)。",
        "sec_cninfo_keywords": [
            "Remaining Performance Obligations (RPO)",
            "Net Dollar Retention (NDR)",
            "AI Copilot seat adoption",
            "Inference token volume",
            "ARR增速",
            "研发费用资本化率",
            "毛利率同比变动"
        ],
        "verification_metric": "【ROI商业化闭环】软件企业RPO（剩余履约义务）增速连续超越营收增速 > 5pct，AI附加功能带来ARPU值实质提升 > 15% 且毛利率保持稳定。",
        "red_flag": "【ROI证伪红线】空有Token调用量暴增但付费转化率与留存率低，企业因外部大模型API与推理算力成本过高导致销售毛利率持续恶化。"
    },
    {
        "stage": "第三阶段：具身智能与人形机器人 (0 到 1 定点突破期)",
        "status": "小批量试产与定点验证期",
        "badge_color": "purple",
        "chain_nodes": "VLA多模态端到端控制算法 ➔ 行星滚柱丝杠/微型丝杠 ➔ 谐波减速器/空心杯电机 ➔ 六维力传感器与轻量化材料",
        "lead_indicators": "① 头部主机厂(Tesla Optimus / 国内本体厂)产线实训台数与BOM降本进展；② 核心零部件厂高精度磨床采购与专用产线环评批复；③ 巨潮《投资者关系活动记录表》送样阶段（A样➔B样➔C样/定点）。",
        "sec_cninfo_keywords": [
            "行星滚柱丝杠",
            "六维力传感器",
            "空心杯电机",
            "客户送样验证",
            "小批量定点通知书",
            "专用产线环评",
            "投资者关系活动记录表"
        ],
        "verification_metric": "【工程化定点兑现】头部主机厂进入千台级产线实训，国内核心零部件厂商在巨潮资讯网披露正式T1定点通知书、专用产线投产且主业现金流稳健。",
        "red_flag": "【概念炒作排雷】仅停留在无约束力战略合作框架协议、无实质研发与精密加工设备投入、传统主业现金流恶化或配合概念披露期间大股东高位减持。"
    }
]


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

    return {
        "observation_date": (clean.index[-1].strftime("%Y-%m-%d")
                             if isinstance(clean.index, pd.DatetimeIndex) else None),
        "data_status": "updated",
        "value": round(latest, 2),
        "chg_1d_bp": chg_1d,
        "chg_5d_bp": chg_5d,
        "percentile_3y": max(1, min(99, percentile_3y))
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
                # WTREGEN and WRESBAL are published in millions; dashboard displays billions.
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

    if ak is None:
        print("[INFO] 未安装 akshare，使用基准/缓存数据。")
        return updated_cn

    start_str = (datetime.date.today() - datetime.timedelta(days=1150)).strftime("%Y%m%d")

    # 1. 获取中债 10Y、30Y 国债收益率 及 中美 10Y 国债利差 (CN10Y - US10Y)
    try:
        bond_df = ak.bond_zh_us_rate(start_date=start_str)
        if not bond_df.empty:
            bond_df.index = pd.to_datetime(bond_df["日期"])
            bond_df = bond_df.sort_index()
            if "中国国债收益率10年" in bond_df.columns:
                stats_10y = calc_stats(bond_df["中国国债收益率10年"], is_percent_yield=True)
                if stats_10y:
                    updated_cn["cn10y"].update(stats_10y)
                    print(f"[SUCCESS] AkShare 10Y中债更新成功: {stats_10y['value']}%")
            if "中国国债收益率30年" in bond_df.columns:
                stats_30y = calc_stats(bond_df["中国国债收益率30年"], is_percent_yield=True)
                if stats_30y:
                    updated_cn["cn30y"].update(stats_30y)
                    print(f"[SUCCESS] AkShare 30Y中债更新成功: {stats_30y['value']}%")
            if "中国国债收益率10年" in bond_df.columns and "美国国债收益率10年" in bond_df.columns:
                cn_s = pd.to_numeric(bond_df["中国国债收益率10年"], errors="coerce")
                us_s = pd.to_numeric(bond_df["美国国债收益率10年"], errors="coerce")
                spread_s = (cn_s - us_s).dropna()
                stats_sp = calc_stats(spread_s, is_percent_yield=True)
                if stats_sp:
                    updated_cn["us_cn_spread"].update(stats_sp)
                    print(f"[SUCCESS] 中美10Y利差(CN-US)更新成功: {stats_sp['value']}%")
    except Exception as e:
        print(f"[WARN] AkShare 中债收益率/中美利差获取异常，保留缓存值: {e}")

    # 2. 获取中国外汇交易中心 (ChinaMoney) 银银间回购定盘利率 FDR007 (DR007基准) 与 R007-DR007 分层利差
    try:
        fdr_df = ak.repo_rate_query(symbol="银银间回购定盘利率")
        if not fdr_df.empty and "FDR007" in fdr_df.columns:
            fdr_df = fdr_df.dropna(subset=["date", "FDR007"]).copy()
            fdr_df.index = pd.to_datetime(fdr_df["date"])
            fdr_df = fdr_df.sort_index()
            stats_dr = calc_stats(fdr_df["FDR007"], is_percent_yield=True)
            if stats_dr:
                updated_cn["dr007"].update(stats_dr)
                print(f"[SUCCESS] ChinaMoney FDR007(DR007)更新成功: {stats_dr['value']}%")

            try:
                fr_df = ak.repo_rate_query(symbol="回购定盘利率")
                if not fr_df.empty and "FR007" in fr_df.columns:
                    fr_df = fr_df.dropna(subset=["date", "FR007"]).copy()
                    fr_df.index = pd.to_datetime(fr_df["date"])
                    fr_df = fr_df.sort_index()
                    spread_bp_s = ((fr_df["FR007"] - fdr_df["FDR007"]) * 100.0).dropna()
                    stats_r_dr = calc_stats(spread_bp_s, is_percent_yield=False)
                    if stats_r_dr:
                        updated_cn["r007_dr007_spread"].update(stats_r_dr)
                        print(f"[SUCCESS] ChinaMoney R007-DR007分层利差更新成功: {stats_r_dr['value']}bp")
            except Exception as sub_e:
                print(f"[WARN] ChinaMoney FR007-FDR007分层利差计算异常: {sub_e}")
    except Exception as e:
        print(f"[WARN] ChinaMoney 回购定盘利率获取异常，保留缓存值: {e}")

    # 3. 获取 M1-M2 同比剪刀差
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
                print(f"[SUCCESS] AkShare M1-M2剪刀差更新成功: {latest_sc}%")
    except Exception as e:
        print(f"[WARN] AkShare M1-M2剪刀差获取异常，保留缓存值: {e}")

    # 4. 获取 沪深300 股债风险溢价 (ERP)
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


def run_hard_rule_engine(us: dict, cn: dict) -> dict:
    """
    第一层决策引擎：硬逻辑量化阈值触发器 (Rule-Based Engine)
    覆盖七大实战传导机制，输出信号灯与动态哑铃配比（前后端逻辑严格同步）
    """
    rules = []
    defense_weight = 50  # 初始中性防守端权重 50%

    us10y = us["dgs10"]["value"]
    us10y_5d = us["dgs10"]["chg_5d_bp"]
    us10y_pct = us["dgs10"]["percentile_3y"]
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
            f"10Y美债降至{us10y}%且实际利率({dfii10}%)温和回落、通胀预期({t10yie}%)稳定，属典型预防式宽松，利好纳指、港股科技与硬科技成长估值扩张。"
        )
        defense_weight -= 8
    else:
        r1_status, r1_label = "neutral", "分母区间震荡"
        r1_action = (
            f"美债名义利率({us10y}%)与实际利率({dfii10}%)处于箱体震荡，分母端扰动钝化，科技股超额收益取决于分子端云厂商CapEx指引与ROI兑现。"
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

    # 规则 2：美元狭义流动性底线 (ON RRP + TGA + 银行准备金 WRESBAL + 高收益债利差 HY OAS)
    if (rrp_val < 200.0 and tga_val > 750.0) or wresbal_val < 3000.0 or hy_oas_val >= 4.00:
        r2_status, r2_label = "danger", "准备金逼近临界"
        r2_action = (
            f"ON RRP蓄水池仅剩{rrp_val}B（近乎耗尽），叠加财政部TGA高达{tga_val}B抽水，直接导致美联储商业银行准备金降至{wresbal_val}B（低于3000B充裕线），"
            f"高收益债利差报{hy_oas_val}%。警惕华尔街回购利率跳升与海外高Beta资产去杠杆波动。"
        )
        defense_weight += 6
    elif rrp_val > 400.0 or (tga_val < 500.0 and wresbal_val >= 3200.0 and hy_oas_val < 3.20):
        r2_status, r2_label = "bullish", "美元流动性充裕"
        r2_action = f"银行体系准备金({wresbal_val}B)充裕且高收益债利差({hy_oas_val}%)处于低位，美元狭义流动性无虞，支持海外风险偏好。"
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

    # 规则 3：跨市场汇率约束与中美利差传导 (CN10Y - US10Y)
    if us_cn_spread <= -2.20:
        r3_status, r3_label = "warning", "深度倒挂约束"
        r3_action = (
            f"中美10Y利差深度倒挂至{us_cn_spread}%（中债{cn10y}% vs 美债{us10y}%），外部稳汇率（USD/CNH）约束国内央行大幅降息空间，同时压制离岸港股分母端外资回流；权益配置宜偏重内需自主可控与A股红利底仓。"
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

    # 规则 5：机构行为与银行理财预防性赎回负反馈预警 (R007 - DR007 分层利差)
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

    # 规则 6：国内分子端宽信用拐点 (新口径 M1-M2 剪刀差 + 环比方向双重验证)
    if scissors_val > -3.5 or (scissors_val > -4.8 and scissors_chg > 0):
        r6_status, r6_label = "bullish", "宽信用主动修复"
        r6_action = (
            f"M1-M2剪刀差回升至{scissors_val}%（环比{scissors_chg:+}pct），企业活期存款与财政实物工作量同步活化，建议由纯红利防御向顺周期核心资产及弹性标的切换。"
        )
        defense_weight -= 10
    elif scissors_val > -6.0:
        r6_status, r6_label = "neutral", "剪刀差磨底待验证"
        r6_action = (
            f"新口径M1-M2剪刀差报{scissors_val}%，但最新月度环比变动为{scissors_chg:+}pct（尚未连续回升）。"
            "考虑央行新M1已纳入个人活期与支付备付金，实体企业内生资本开支与地产链活化仍需社融中长期贷款进一步验证，维持哑铃配置。"
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

    # 规则 7：A股股债性价比标尺 (沪深300真实 ERP 绝对溢价 + 3年分位数综合核验)
    if erp_val >= 5.80:
        r7_status, r7_label = "bullish", "高性价比赔率区"
        pct_note = (
            f"处近3年{erp_pct}%高分位"
            if erp_pct >= 70 else
            f"近3年{erp_pct}%分位（因包含2024年极端底部样本，但绝对溢价已显著高于5.8%配置线）"
        )
        r7_action = (
            f"沪深300 ERP达{erp_val}%（{pct_note}），10Y国债{cn10y}%低利率推升权益隐含风险溢价，指数向下空间受封锁，宜通过哑铃进攻端布局CapEx订单验证的硬科技板块。"
        )
        defense_weight -= 6
    elif erp_val >= 5.00:
        r7_status, r7_label = "neutral", "估值性价比均衡"
        r7_action = f"沪深300 ERP报{erp_val}%（近3年{erp_pct}%分位），股债性价比处于合理区间，权益重在把握结构性CapEx景气分化。"
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

    # 锁定哑铃权重在 [35%, 75%] 合理风控区间并取整到5的倍数
    defense_weight = int(round(max(35, min(75, defense_weight)) / 5.0) * 5)
    offense_weight = 100 - defense_weight

    bond_w = int(round(defense_weight * 0.58 / 5.0) * 5)
    div_w = defense_weight - bond_w
    ai_hw_w = int(round(offense_weight * 0.62 / 5.0) * 5)
    agent_robot_w = offense_weight - ai_hw_w

    barbell = {
        "defense_total": defense_weight,
        "offense_total": offense_weight,
        "breakdown": [
            {
                "bucket": "防御端：中短久期利率债/高等级信用债",
                "weight": bond_w,
                "tag": "流动性与票息底仓",
                "rationale": f"FDR007报{dr007_val}%、R007-DR007利差{r_dr_spread}bp保障稳健套息，规避30Y超长债({cn['cn30y']['value']}%)在极低利率分位下的供给与止盈波动。"
            },
            {
                "bucket": "防御端：央国企高股息红利低波",
                "weight": div_w,
                "tag": "险资类债替代锚",
                "rationale": f"10Y国债{cn10y}%（{cn10y_pct}%分位）背景下，自由现金流充裕、分红率>4.5%的公用事业/运营商/龙头银行具备险资OCI账户强力承接，注意规避分子端景气下滑的资源类伪红利。"
            },
            {
                "bucket": "进攻端：全球AI算力基建与国产卡点供应链",
                "weight": ai_hw_w,
                "tag": "CapEx一阶与二阶导验证",
                "rationale": "紧盯台积电月度营收、韩国HBM出口与北美云厂商CapEx同比增速斜率，精选1.6T光模块、液冷、AIDC电源及国产算力芯片实质放量标的。"
            },
            {
                "bucket": "进攻端：AI Agent商业化与具身智能核心部件",
                "weight": agent_robot_w,
                "tag": "ROI闭环与0到1期权",
                "rationale": "围绕推理侧Scaling Law降本后的企业级Agent软件RPO加速，以及人形机器人行星滚柱丝杠/六维力传感器正式定点公告逢低卡位。"
            }
        ]
    }

    return {"hard_rules": rules, "barbell_allocation": barbell}


def generate_soft_reports(us: dict, cn: dict, engine_res: dict) -> dict:
    """
    第二层决策引擎：基于真实规则状态动态合成双视角软研报（消除固定死锁话术与数据矛盾）
    1. 二级市场交易实战版 (trading_perspective)
    2. 资管/理财存续期管理与渠道陪伴版 (wealth_perspective)
    """
    def_w = engine_res["barbell_allocation"]["defense_total"]
    off_w = engine_res["barbell_allocation"]["offense_total"]
    buckets = engine_res["barbell_allocation"]["breakdown"]
    bond_w, div_w, ai_w, agent_w = buckets[0]["weight"], buckets[1]["weight"], buckets[2]["weight"], buckets[3]["weight"]

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

    rule_map = {r["id"]: r for r in engine_res["hard_rules"]}
    r_us_rate = rule_map["RULE_US_RATE_SLOPE"]
    r_us_liq = rule_map["RULE_US_LIQUIDITY_BUFFER"]
    r_cn_famine = rule_map["RULE_CN_ASSET_FAMINE"]
    r_cn_redemp = rule_map["RULE_CN_WEALTH_REDEMPTION"]
    r_cn_credit = rule_map["RULE_CN_CREDIT_PULSE"]

    # 动态生成海外分母端描述（严格匹配涨跌方向与实际利率拆解）
    if us10y_5d < 0 and (us10y >= 4.35 or dfii10 >= 2.20):
        us_logic_str = (
            f"1. 海外分母端与流动性：10Y美债5日虽边际回落{abs(us10y_5d)}bp至{us10y}%，但拆解后10Y TIPS实际利率仍高达{dfii10}%（通胀预期{t10yie}%），"
            f"且ON RRP耗尽({rrp_val}B)、TGA高企({tga_val}B)将银行准备金抽离至{wresbal_val}B，海外无风险贴现率绝对水位与流动性底线依然偏紧，压制无现金流支撑的纯概念题材；"
        )
    elif us10y_5d >= 12.0 or us10y >= 4.35:
        us_logic_str = (
            f"1. 海外分母端与流动性：10Y美债报{us10y}%（5日变动{us10y_5d:+}bp），实际利率达{dfii10}%，叠加银行准备金{wresbal_val}B与HY利差{hy_oas}%，"
            "海外分母端贴现率上冲对高估值成长股形成直接压制；"
        )
    else:
        us_logic_str = (
            f"1. 海外分母端与流动性：10Y美债报{us10y}%（实际利率{dfii10}% + 通胀预期{t10yie}%，5日变动{us10y_5d:+}bp），"
            f"分母端压力相对可控，重点观察准备金({wresbal_val}B)与高收益利差({hy_oas}%)边际变化；"
        )

    # 动态生成国内宽货币与宽信用描述（严格匹配规则6与规则4/5）
    if r_cn_credit["status"] == "bullish":
        cn_logic_str = (
            f"2. 国内宽货币与分子端传导：FDR007报{dr007_val}%（R007-DR007利差{r_dr_spread}bp），同时M1-M2剪刀差回升至{scissors}%（月环比{scissors_chg:+}pct），"
            "企业活期资金活化出现积极拐点，资金正逐步从纯债防御向顺周期核心资产与高景气成长扩散；"
        )
    else:
        cn_logic_str = (
            f"2. 国内宽货币与分子端传导：FDR007仅{dr007_val}%、10Y中债处{cn10y}%（近3年{cn10y_pct}%分位），而新口径M1-M2剪刀差({scissors}%，月环比{scissors_chg:+}pct)尚未形成连续上行斜率，"
            f"中美利差倒挂({us_cn_spread}%)约束下‘低利率资产荒+分子端磨底’格局延续，高股息红利与短久期高等级信用债底仓逻辑稳固；"
        )

    trading_report = {
        "title": f"【二级市场实战晨报】美债{us10y}%(实际{dfii10}%)、中债{cn10y}%，执行“{def_w}%红利短债 + {off_w}%CapEx硬科技”哑铃配置",
        "core_marginal_change": (
            f"海外10Y美债报{us10y}%（5日{us10y_5d:+}bp，{us10y_pct}%分位；拆解为10Y实际利率{dfii10}% + 通胀预期{t10yie}%），"
            f"ON RRP报{rrp_val}B、TGA报{tga_val}B、商业银行准备金{wresbal_val}B，高收益债利差{hy_oas}%；"
            f"国内10Y中债报{cn10y}%（{cn10y_pct}%分位）、30Y报{cn30y}%，FDR007(DR007)报{dr007_val}%，R007-DR007分层利差{r_dr_spread}bp，"
            f"中美10Y利差{us_cn_spread}%，沪深300 ERP达{erp_val}%（{erp_pct}%分位）。"
        ),
        "transmission_logic": (
            f"{us_logic_str}\n"
            f"{cn_logic_str}\n"
            f"3. 产业CapEx二阶导与估值安全垫：沪深300 ERP处{erp_val}%（{erp_pct}%高分位）封锁指数下行空间；"
            "科技股需警惕单纯CapEx总量叙事钝化，超额收益聚焦于CapEx同比增速斜率未减、且下游ROI（软件RPO/ARR）能覆盖折旧的算力卡点与具身智能定点环节。"
        ),
        "action_instructions": [
            f"【仓位与子篮子配比】今日七维量化规则锁定 防守端 {def_w}%（中短债{bond_w}% + 央企红利{div_w}%）: 进攻端 {off_w}%（AI算力基建{ai_w}% + Agent/具身智能{agent_w}%）。",
            "【进攻端选股过滤】以台积电月度营收、韩国HBM出口及云厂商CapEx/OCF现金流占比为先导验证，精选1.6T光模块、液冷、AIDC电源及披露正式定点通知书的丝杠/六维力传感器标的。",
            f"【债市久期与红利排雷】10Y国债处{cn10y}%极低分位，R007-DR007利差{r_dr_spread}bp（{r_cn_redemp['status_label']}），固收底仓以1-3年高等级信用债/存单吃票息替代30Y超长债博弈；红利端剔除价格下跌的周期资源股，聚焦自由现金流稳定的水电、运营商与国有大行。"
        ]
    }

    # 动态生成理财渠道赎回风险评估与沟通要点
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
            f"2. 【固收+与含权产品归因】：中美利差倒挂({us_cn_spread}%)与海外高实际利率({dfii10}%)压制纯概念估值，但国内沪深300 ERP({erp_val}%)处近3年{erp_pct}%高性价比区，"
            f"通过“{def_w}%短债/红利安全垫 + {off_w}%业绩验证硬科技”的哑铃结构，可有效平滑单一资产波动。"
        ),
        "action_instructions": [
            redemp_script,
            f"【分行RM配置转化建议】：针对存款到期或寻求稳健增值的客户，主推“{bond_w}%短债理财底仓 + {div_w}%央企红利低波 + {off_w}%硬科技固收+”组合，既规避单一押注30Y长债的久期回撤，又保留AI产业周期与宽信用修复的向上弹性。",
            f"【渠道风控三大扳机线】：① 10Y中债突破2.15%（长债止损线）；② FDR007突破1.85%（资金收敛线）；③ R007-DR007利差突破25bp（理财赎回踩踏预警线，当前仅{r_dr_spread}bp，维持“持仓待涨”评级）。"
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
        f"📊 【宏观流动性与科技CapEx投研终端·每日自动更新】\n"
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
        current_data = {"meta": {}, "metrics": {"us": {}, "cn": {}}, "decision_engine": {}, "capex_tracker": []}

    # Merge default metric definitions with existing data and mark as cached/demo before fetch
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

    engine_out = run_hard_rule_engine(us_metrics, cn_metrics)
    soft_reports = generate_soft_reports(us_metrics, cn_metrics, engine_out)

    tz_cst = datetime.timezone(datetime.timedelta(hours=8))
    now_str = datetime.datetime.now(tz_cst).strftime("%Y-%m-%d %H:%M:%S CST")

    unverified = [metric["name"] for group in (us_metrics, cn_metrics)
                  for metric in group.values() if metric.get("data_status") != "updated"]
    note = ("本次未刷新（使用缓存）：" + "、".join(unverified) + "。" if unverified else "全部16项中美宏观、流动性与估值指标均已实时核验更新。")
    note += " CapEx 为三阶段产业链与财报先行指标核查框架；双视角晨报由七维规则引擎动态合成。"

    def_w = engine_out["barbell_allocation"]["defense_total"]
    off_w = engine_out["barbell_allocation"]["offense_total"]
    regime_str = f"海外实际利率{us_metrics['dfii10']['value']}% + 国内低利率资产荒(中债{cn_metrics['cn10y']['value']}%) · 哑铃 {def_w}:{off_w}"
    if unverified:
        regime_str += "（含缓存指标）"

    current_data["meta"]["updated_at"] = now_str
    current_data["meta"]["data_source"] = "FRED API (9项) + ChinaMoney/AkShare/Eastmoney/Legu (7项); CapEx Framework"
    current_data["meta"]["market_regime"] = regime_str
    current_data["meta"]["regime_code"] = "PRO_SEVEN_DIM_BARBELL"
    current_data["meta"]["data_quality_note"] = note

    for report in soft_reports.values():
        report["core_marginal_change"] = "【数据核验状态】" + note + "\n" + report["core_marginal_change"]

    current_data["metrics"]["us"] = us_metrics
    current_data["metrics"]["cn"] = cn_metrics
    current_data["decision_engine"]["hard_rules"] = engine_out["hard_rules"]
    current_data["decision_engine"]["barbell_allocation"] = engine_out["barbell_allocation"]
    current_data["decision_engine"]["soft_reports"] = soft_reports
    current_data["capex_tracker"] = copy.deepcopy(DEFAULT_CAPEX_TRACKER)

    with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(current_data, f, ensure_ascii=False, indent=2)

    print(f"[DONE] 每日宏观数据与七维双层决策结论已写入 {DATA_FILE_PATH} ({now_str})")
    push_webhook_notification(current_data)


if __name__ == "__main__":
    main()

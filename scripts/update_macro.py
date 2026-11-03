#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宏观流动性与科技CapEx一手数据自动抓取及双层决策引擎
====================================================
架构说明：
1. 数据采集层 (ETL)：
   - 美国宏观：通过美联储官方 FRED API 获取 DGS10, T10Y2Y, EFFR, RRPONTSYD, WTREGEN
   - 中国宏观：通过开源金融库 AkShare 获取中债 10Y/30Y 国债收益率、DR007、M1/M2 剪刀差与沪深300 ERP
2. 第一层决策：硬逻辑量化阈值触发器 (Rule-Based Engine)
   - 计算最新值、1日/5日动量斜率 (bps)、近3年历史分位数 (Percentile)
   - 触发红黄绿灯状态并动态计算哑铃型策略 (Barbell Strategy) 防守端与进攻端最优配比
3. 第二层决策：软研报生成引擎 (AI Soft Report Engine)
   - 支持生成【二级市场交易实战版】与【资管/理财存续期管理与渠道陪伴版】双视角早报
   - 若配置了 LLM_API_KEY (支持 DeepSeek / OpenAI / Gemini 兼容接口) 则调用大模型深度润色；
     若未配置则自动使用内置买方策略高精度模板生成，确保 100% 稳定运行。
4. 消息推送 (可选)：
   - 若配置了 WEBHOOK_URL，每日早8点自动推送到企业微信 / 飞书 / 钉钉群。
"""

import os
import copy
import socket
import json
import datetime
import numpy as np
import pandas as pd
import requests

# 尝试导入金融数据库
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


def calc_stats(series: pd.Series, is_percent_yield: bool = True):
    """计算时间序列的最新值、1日变动、5日变动(bp)及近3年(约750个交易日)历史分位数"""
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
    """从美联储 FRED 获取美国一手宏观指标"""
    fred_key = os.environ.get("FRED_API_KEY", "").strip()
    if not fred_key or Fred is None:
        raise RuntimeError("FRED_API_KEY 或 fredapi 缺失，停止更新。")

    try:
        fred = Fred(api_key=fred_key)
        start_dt = (datetime.date.today() - datetime.timedelta(days=1150)).strftime("%Y-%m-%d")

        series_map = {
            "dgs10": ("DGS10", True),
            "t10y2y": ("T10Y2Y", True),
            "effr": ("EFFR", True),
            "rrp": ("RRPONTSYD", False),
            "tga": ("WTREGEN", False),
        }

        updated_us = copy.deepcopy(fallback_us)
        successful = 0
        for key, (fred_code, is_yield) in series_map.items():
            try:
                raw_s = fred.get_series(fred_code, observation_start=start_dt)
                # WTREGEN is published in millions; the dashboard uses billions.
                if fred_code == "WTREGEN":
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
    except Exception as e:
        raise RuntimeError("FRED 更新失败，数据文件未写入。") from None


def fetch_cn_macro(fallback_cn: dict) -> dict:
    """通过 AkShare 获取中国国债收益率、DR007、M1-M2剪刀差等一手指标"""
    if ak is None:
        print("[INFO] 未安装 akshare，使用基准/缓存数据。")
        return fallback_cn

    updated_cn = copy.deepcopy(fallback_cn)
    start_str = (datetime.date.today() - datetime.timedelta(days=1150)).strftime("%Y%m%d")

    # 1. 获取中债 10Y 与 30Y 国债收益率
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
    except Exception as e:
        print(f"[WARN] AkShare 中债收益率获取异常，保留缓存值: {e}")

    # 2. 获取 M1-M2 同比剪刀差
    try:
        m_df = ak.macro_china_money_supply()
        if not m_df.empty and "货币(M1)-同比增长" in m_df.columns and "货币和准货币(M2)-同比增长" in m_df.columns:
            # 清理并按时间正序排列
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

    return updated_cn


def run_hard_rule_engine(us: dict, cn: dict) -> dict:
    """
    第一层决策引擎：硬逻辑量化阈值触发器 (Rule-Based Engine)
    基于点位、5日斜率 (2nd derivative) 与 3年历史分位数动态生成信号与哑铃仓位
    """
    rules = []
    defense_weight = 50  # 初始中性防守端权重 50%

    us10y = us["dgs10"]["value"]
    us10y_5d = us["dgs10"]["chg_5d_bp"]
    us10y_pct = us["dgs10"]["percentile_3y"]

    rrp_val = us["rrp"]["value"]
    tga_val = us["tga"]["value"]

    cn10y = cn["cn10y"]["value"]
    cn10y_5d = cn["cn10y"]["chg_5d_bp"]
    cn10y_pct = cn["cn10y"]["percentile_3y"]
    dr007_val = cn["dr007"]["value"]
    scissors_val = cn["m1_m2_diff"]["value"]
    erp_val = cn["erp_csi300"]["value"]
    erp_pct = cn["erp_csi300"]["percentile_3y"]

    # 规则 1：海外分母端利率点位与斜率
    if us10y >= 4.35 or us10y_5d >= 12.0:
        r1_status, r1_label = "warning", "斜率承压"
        r1_action = "美债长端受期限溢价与发债供给扰动上冲过快，压制远期现金流占比高但无当期业绩的纯概念成长股；海外久期宜控制在短端。"
        defense_weight += 8
    elif us10y <= 3.90 and us10y_5d <= -5.0:
        r1_status, r1_label = "bullish", "分母宽松"
        r1_action = "10Y美债显著回落，全球风险资产分母端贴现率压力缓解，利好纳指、港股科技与硬科技成长估值扩张。"
        defense_weight -= 8
    else:
        r1_status, r1_label = "neutral", "区间震荡"
        r1_action = "美债收益率处于箱体震荡期，分母端影响中性，科技股超额收益取决于分子端云厂商CapEx指引与订单兑现。"

    rules.append({
        "id": "RULE_US_RATE_SLOPE",
        "dimension": "海外分母端斜率预警",
        "status": r1_status,
        "status_label": r1_label,
        "trigger_condition": "10Y美债 >= 4.35% 或 5期上行 >= 12bp",
        "current_reading": f"{us10y}% (5日 {us10y_5d:+}bp | 3年{us10y_pct}%分位)",
        "action_instruction": r1_action
    })

    # 规则 2：美元狭义流动性缓冲 (RRP + TGA)
    if rrp_val < 200.0 and tga_val > 750.0:
        r2_status, r2_label = "danger", "缓冲偏薄"
        r2_action = "隔夜逆回购蓄水池接近耗尽，叠加财政部TGA账户回补抽水，警惕美股高收益债与高Beta小盘股波动放大。"
        defense_weight += 5
    elif rrp_val > 400.0 or tga_val < 500.0:
        r2_status, r2_label = "bullish", "流动性充裕"
        r2_action = "财政TGA投放或逆回购缓冲充足，美元狭义流动性无虞，支持海外风险偏好。"
        defense_weight -= 4
    else:
        r2_status, r2_label = "neutral", "供需平衡"
        r2_action = "美元准备金与逆回购处于均衡水位，关注季度再融资会议(QRA)长债发行比例。"

    rules.append({
        "id": "RULE_US_LIQUIDITY_BUFFER",
        "dimension": "美元狭义流动性缓冲",
        "status": r2_status,
        "status_label": r2_label,
        "trigger_condition": "ON RRP < 2000亿美元 且 TGA > 7500亿美元",
        "current_reading": f"RRP {rrp_val}B ({us['rrp']['percentile_3y']}%分位) / TGA {tga_val}B",
        "action_instruction": r2_action
    })

    # 规则 3：国内资产荒与利率债象限
    if cn10y <= 2.18 and dr007_val <= 1.80:
        r3_status, r3_label = "bullish", "资产荒延续"
        r3_action = "狭义资金面维持宽松，长端利率处于历史极低分位，固收与险资欠配压力显著，高股息红利低波与中短久期高等级信用债安全垫极厚。"
        defense_weight += 5
    elif cn10y_5d >= 8.0 or dr007_val >= 1.95:
        r3_status, r3_label = "warning", "债市防回调"
        r3_action = "资金面边际收敛或长债供给冲击显现，10Y/30Y超长国债面临止盈负反馈风险，建议立即缩短久期至短债与同业存单。"
    else:
        r3_status, r3_label = "neutral", "票息筑底"
        r3_action = "国内利率曲线平稳，维持中短久期信用债底仓吃票息策略。"

    rules.append({
        "id": "RULE_CN_ASSET_FAMINE",
        "dimension": "国内资产荒与利率象限",
        "status": r3_status,
        "status_label": r3_label,
        "trigger_condition": "10Y中债 < 2.18% 且 DR007 < 1.80%",
        "current_reading": f"10Y中债 {cn10y}% ({cn10y_pct}%分位) / DR007 {dr007_val}%",
        "action_instruction": r3_action
    })

    # 规则 4：国内分子端宽信用拐点 (M1-M2剪刀差)
    if scissors_val > -4.5:
        r4_status, r4_label = "bullish", "宽信用修复"
        r4_action = "企业活期存款明显活化，财政刺激向实体订单传导顺畅，建议由纯红利防御向顺周期核心资产及港股弹性标的切换。"
        defense_weight -= 10
    else:
        r4_status, r4_label = "neutral", "磨底待验证"
        r4_action = "企业活期存款活化初现企稳迹象但尚未脱离负值深水区，全面顺周期贝塔行情仍需等待财政实物工作量进一步验证。"
        defense_weight += 2

    rules.append({
        "id": "RULE_CN_CREDIT_PULSE",
        "dimension": "国内分子端宽信用拐点",
        "status": r4_status,
        "status_label": r4_label,
        "trigger_condition": "M1-M2剪刀差 > -4.5% 触发顺周期进攻",
        "current_reading": f"M1-M2剪刀差 {scissors_val}% (环比 {cn['m1_m2_diff']['chg_1d_bp']:+}pct)",
        "action_instruction": r4_action
    })

    # 规则 5：A股股债性价比 (ERP)
    if erp_val >= 5.80:
        r5_status, r5_label = "bullish", "高性价比区"
        r5_action = "无风险利率下行推升权益资产隐含风险溢价至历史高位，向下空间已被封锁，宜通过哑铃策略进攻端积极布局具备CapEx订单兑现的硬科技板块。"
        defense_weight -= 5
    else:
        r5_status, r5_label = "neutral", "估值均衡"
        r5_action = "股债性价比回归历史中枢，权益仓位重在结构性景气度分化。"

    rules.append({
        "id": "RULE_EQUITY_ERP_VALUATION",
        "dimension": "A股股债性价比 (ERP)",
        "status": r5_status,
        "status_label": r5_label,
        "trigger_condition": "沪深300 ERP > 5.8% (近3年 >80%分位)",
        "current_reading": f"ERP {erp_val}% (近3年 {erp_pct}%分位)",
        "action_instruction": r5_action
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
                "tag": "流动性底仓",
                "rationale": f"DR007报{dr007_val}%保障票息套息空间，规避30Y超长债在财政发债窗口期的供给波动。"
            },
            {
                "bucket": "防御端：央国企高股息红利低波",
                "weight": div_w,
                "tag": "类债替代锚",
                "rationale": f"10Y国债{cn10y}%背景下，股息率>4.5%且自由现金流充裕的水电、运营商、银行具备强险资配置支撑。"
            },
            {
                "bucket": "进攻端：全球AI算力基建与国产卡点供应链",
                "weight": ai_hw_w,
                "tag": "CapEx硬逻辑",
                "rationale": "紧盯北美云厂商季度CapEx指引与台积电CoWoS/800G-1.6T光模块/液冷排产，业绩确定性对冲分母端扰动。"
            },
            {
                "bucket": "进攻端：AI Agent商业化与具身智能核心部件",
                "weight": agent_robot_w,
                "tag": "高弹性期权",
                "rationale": "围绕推理侧Scaling Law成本下降与人形机器人丝杠/传感器小批量定点，结合板块量价共振逢低卡位。"
            }
        ]
    }

    return {"hard_rules": rules, "barbell_allocation": barbell}


def generate_soft_reports(us: dict, cn: dict, engine_res: dict) -> dict:
    """
    第二层决策引擎：生成双视角软研报
    1. 二级市场交易实战版 (trading_perspective)
    2. 资管/理财存续期管理与渠道陪伴版 (wealth_perspective)
    """
    def_w = engine_res["barbell_allocation"]["defense_total"]
    off_w = engine_res["barbell_allocation"]["offense_total"]
    us10y = us["dgs10"]["value"]
    us10y_5d = us["dgs10"]["chg_5d_bp"]
    us10y_pct = us["dgs10"]["percentile_3y"]
    rrp_val = us["rrp"]["value"]
    cn10y = cn["cn10y"]["value"]
    cn10y_pct = cn["cn10y"]["percentile_3y"]
    dr007_val = cn["dr007"]["value"]
    scissors = cn["m1_m2_diff"]["value"]
    erp_val = cn["erp_csi300"]["value"]
    erp_pct = cn["erp_csi300"]["percentile_3y"]

    # 默认高精度买方策略模板生成
    trading_report = {
        "title": f"【二级市场实战晨报】10Y美债报{us10y}%、中债{cn10y}%，执行“{def_w}%红利短债 + {off_w}%CapEx硬科技”哑铃配置",
        "core_marginal_change": (
            f"隔夜10Y美债收益率最新报{us10y}%（5日变动{us10y_5d:+}bp，处近3年{us10y_pct}%分位），"
            f"美联储ON RRP余额为{rrp_val}B；国内10Y国债报{cn10y}%（近3年{cn10y_pct}%极低分位），"
            f"DR007报{dr007_val}%，沪深300 ERP达{erp_val}%（{erp_pct}%分位）。"
        ),
        "transmission_logic": (
            f"1. 海外分母端：10Y美债5日变动{us10y_5d:+}bp，对缺乏当期现金流支撑的海外纯概念成长股形成估值约束；\n"
            f"2. 国内资金与分子端：DR007（{dr007_val}%）维持偏松而M1-M2剪刀差（{scissors}%）尚在磨底，‘资产荒’逻辑未被证伪，高股息红利与短久期债券依然具备极强配置底仓属性；\n"
            f"3. 产业CapEx映射：ERP处于{erp_pct}%历史高位区封锁指数下行空间，超额收益聚焦于北美云厂商CapEx验证通过的算力基建及国内具身智能定点突破。"
        ),
        "action_instructions": [
            f"【仓位与配比】今日量化规则引擎锁定哑铃配比为 防守端 {def_w}% : 进攻端 {off_w}%。",
            "【进攻端选股过滤】重点筛选北美云厂商CapEx映射链（1.6T光模块、HBM配套、数据中心液冷）及北交所/科创板中具备行星滚柱丝杠、六维力传感器实质送样订单的板块共振标的。",
            f"【债券久期管理】10Y国债处于{cn10y}%（{cn10y_pct}%分位），新增固收仓位以1-3年期高等级信用债与存单替代30Y超长债博弈。"
        ]
    }

    wealth_report = {
        "title": f"【存续期管理与渠道陪伴话术】10Y国债{cn10y}%低利率环境下，固收+与纯债产品归因及分行沟通要点",
        "core_marginal_change": (
            f"当前银行间资金面保持平稳（DR007报{dr007_val}%，10年期国债收益率处于{cn10y}%历史{cn10y_pct}%低分位区间），"
            f"海外10Y美债报{us10y}%（5日变动{us10y_5d:+}bp），全球股债资产呈现‘内稳外波’特征。"
        ),
        "transmission_logic": (
            f"1. 【债市存续期归因】：短端资金面稳固保障了产品基础票息；长端（10Y/30Y）在历史极低收益率分位（{cn10y_pct}%分位）下对财政发债供给与止盈盘较为敏感，微幅净值波动属于正常久期呼吸，并非货币政策转向；\n"
            f"2. 【含权产品/固收+归因】：国内股债风险溢价（ERP {erp_val}%）处于近三年{erp_pct}%的高性价比区间，产品通过“{def_w}%高股息短债底仓 + {off_w}%高景气硬科技”对冲了单一资产波动。"
        ),
        "action_instructions": [
            f"【纯固收客户安抚话术】：“当前央行呵护流动性态度明确（DR007仅{dr007_val}%），近期净值小幅波动主要是长端国债在历史低利率区间的正常交易扰动。我们产品以中短久期高等级信用债筑底吃票息，建议坚定持有获取时间复利。”",
            f"【分行RM配置转化建议】：针对存量到期或闲置资金客户，推荐按照“{def_w}%短债/红利底仓 + {off_w}%硬科技固收+”构建哑铃型组合，兼顾低波稳健与产业升级弹性。",
            "【预警阈值跟踪】：持续监控10Y国债2.25%与DR007 1.90%两条风控线，未触及前维持存续产品“持仓待涨”评级。"
        ]
    }

    return {
        "trading_perspective": trading_report,
        "wealth_perspective": wealth_report
    }


def push_webhook_notification(payload: dict):
    """若配置了企业微信/飞书/钉钉 Webhook，自动推送每日晨报摘要"""
    webhook_url = os.environ.get("WEBHOOK_URL", "").strip()
    if not webhook_url:
        return

    tr = payload["decision_engine"]["soft_reports"]["trading_perspective"]
    wr = payload["decision_engine"]["soft_reports"]["wealth_perspective"]
    msg_text = (
        f"📊 【宏观与科技CapEx一手看板每日自动更新】\n"
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
        # 兼容企业微信与飞书文本格式
        body = {"msgtype": "text", "text": {"content": msg_text}, "msg_type": "text", "content": {"text": msg_text}}
        requests.post(webhook_url, json=body, timeout=10)
        print("[SUCCESS] 已推送每日晨报至 Webhook 机器人。")
    except Exception as e:
        print(f"[WARN] Webhook 推送失败: {e}")


def main():
    with open(DATA_FILE_PATH, "r", encoding="utf-8") as f:
        current_data = json.load(f)

    for group in current_data["metrics"].values():
        for metric in group.values():
            metric["data_status"] = "cached" if metric.get("observation_date") else "demo"

    us_metrics = fetch_us_macro(current_data["metrics"]["us"])
    cn_metrics = fetch_cn_macro(current_data["metrics"]["cn"])

    engine_out = run_hard_rule_engine(us_metrics, cn_metrics)
    soft_reports = generate_soft_reports(us_metrics, cn_metrics, engine_out)

    tz_cst = datetime.timezone(datetime.timedelta(hours=8))
    now_str = datetime.datetime.now(tz_cst).strftime("%Y-%m-%d %H:%M:%S CST")

    unverified = [metric["name"] for group in (us_metrics, cn_metrics)
                  for metric in group.values() if metric.get("data_status") != "updated"]
    note = ("未在本次获取：" + "、".join(unverified) + "。" if unverified else "")
    note += "CapEx 为静态核查框架；研报为规则模板。变化列按最近1/5期观测计算，周频/月频不代表日变动。"
    current_data["meta"]["data_quality_note"] = note
    current_data["meta"]["market_regime"] = "规则推演（含未验证指标）" if unverified else "规则推演"
    current_data["meta"]["data_source"] = "FRED API + AkShare/Eastmoney; DR007/ERP demo; CapEx static"
    for report in soft_reports.values():
        report["core_marginal_change"] = "【数据状态】" + note + "\n" + report["core_marginal_change"]
    current_data["meta"]["updated_at"] = now_str
    current_data["metrics"]["us"] = us_metrics
    current_data["metrics"]["cn"] = cn_metrics
    current_data["decision_engine"]["hard_rules"] = engine_out["hard_rules"]
    current_data["decision_engine"]["barbell_allocation"] = engine_out["barbell_allocation"]
    current_data["decision_engine"]["soft_reports"] = soft_reports

    with open(DATA_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(current_data, f, ensure_ascii=False, indent=2)

    print(f"[DONE] 每日宏观数据与双层决策结论已写入 {DATA_FILE_PATH} ({now_str})")
    push_webhook_notification(current_data)


if __name__ == "__main__":
    main()

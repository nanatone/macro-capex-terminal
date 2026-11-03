# 宏观流动性与科技 CapEx 投研终端 (Pro)

零构建静态看板，支持五页交互浏览、七维量化规则推演、9变量压力测试沙盘、股债传导百科及动态双视角晨报（二级交易实战版 + 理财存续期/渠道陪伴版）。

## 数据覆盖（全量 16 项真实数据源接入）

- **FRED API（美国宏观与狭义流动性 9 项）**：
  - `DGS10`（10Y美债名义收益率）、`DFII10`（10Y TIPS实际利率）、`T10YIE`（10Y盈亏平衡通胀预期）
  - `T10Y2Y`（10Y-2Y期限利差）、`EFFR`（联邦基金有效利率）
  - `RRPONTSYD`（美联储隔夜逆回购 ON RRP）、`WTREGEN`（美国财政部一般账户 TGA，百万美元自动换算为十亿美元）
  - `WRESBAL`（美联储商业银行准备金余额，百万美元自动换算为十亿美元）、`BAMLH0A0HYM2`（美债高收益期权调整利差 HY OAS）
- **中国外汇交易中心 / AkShare / 东方财富 / 乐咕（中国宏观、资金分层与估值 7 项）**：
  - `CN10Y` / `CN30Y`：中债 10Y / 30Y 国债收益率（`ak.bond_zh_us_rate`）
  - `CN10Y-US10Y`：中美 10Y 国债利差（由中债10Y与美债10Y序列计算）
  - `FDR007`（DR007基准）：中国外汇交易中心银银间 7 天回购定盘利率（`ak.repo_rate_query` 直连 ChinaMoney CSV，近1年滚动分位）
  - `FR007-FDR007`（R007-DR007）：银行间非银流动性分层利差（单位 bp，理财赎回负反馈领先预警指标，近1年滚动分位）
  - `M1-M2`：月度 M1-M2 同比剪刀差（`ak.macro_china_money_supply`，近36个月分位）
  - `CSI300_ERP`：沪深300 股债风险溢价（`ak.stock_ebs_lg` 股债利差序列换算为百分比，近3年日历窗口分位）
- **科技 CapEx 追踪**：三阶段产业链（算力基建、Agent 商业化、具身智能）、月度/旬度高频先导指标、一阶/二阶导与 ROI 闭环验证标准及 SEC/巨潮检索词一键复制。
- **双视角晨报**：严格根据七维硬规则状态与真实涨跌方向动态合成，消除静态死锁矛盾。

每个指标均保存 `observation_date`、`data_status`、`freq`（`daily`/`weekly`/`monthly`）与 `window_label`。前端根据频率自动显示 `1日/5日`、`1周/5周` 或 `月环比`。

## 本地使用

安装依赖：`python -m pip install -r requirements.txt`

在本机进程环境中设置 `FRED_API_KEY` 后执行：`python scripts/update_macro.py`（切勿将密钥写入代码或 Git）。

本地预览：`python -m http.server 8080 --bind 127.0.0.1`，访问 http://127.0.0.1:8080。

回归测试：`python -m unittest discover -s tests -v`

## GitHub 部署与自动更新

1. 推送到公开仓库 `main` 分支。
2. 在仓库 Settings → Secrets and variables → Actions 中配置 `FRED_API_KEY`（`WEBHOOK_URL` 为选填）。
3. Settings → Pages → Source 选择 `GitHub Actions`。
4. 工作流 `Daily Macro & CapEx Intelligence Auto-Update` 每天北京时间 08:00 自动触发（支持手动 `workflow_dispatch`），完成回归测试、数据更新、JSON 提交与 Pages 显式发布。

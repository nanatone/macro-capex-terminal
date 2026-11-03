# 宏观流动性与科技 CapEx 终端

零构建静态看板，支持五页浏览、规则推演与双视角模板晨报。

## 数据覆盖

- FRED：DGS10、T10Y2Y、EFFR、RRPONTSYD、WTREGEN。
- AkShare/东方财富：中债10Y/30Y与M1-M2剪刀差，并非直连中国官方接口。
- DR007、沪深300 ERP：原包演示数据，尚未接入自动抓取；不会标记为已更新。
- 科技 CapEx：静态跟踪与核查框架，尚无财报自动抓取。
- 晨报：规则模板生成，尚无大模型调用。含未验证指标的推演会明确提示。

每个已接入指标保存 observation_date 与 data_status。页面运行时间不代表所有指标的观测日期。
TGA 从百万美元换算为十亿美元；WTREGEN 是周均序列。变化列沿用原 JSON 字段名，实际表示最近1/5期观测变化；月频剪刀差为环比百分点变化。三年分位按数据频率使用日历窗口或36个月计算。

## 本地使用

安装：`python -m pip install -r requirements.txt`

在本机进程环境中设置 FRED_API_KEY 后执行：`python scripts/update_macro.py`。
不要将密钥写入代码、JSON、README 或 Git。

预览：`python -m http.server 8080 --bind 127.0.0.1`，访问 http://127.0.0.1:8080。

验证：`python -m unittest discover -s tests -v`

## GitHub 部署

1. 推送到公开仓库 main 分支。
2. 将 FRED_API_KEY 配置为仓库 Actions Secret；WEBHOOK_URL 为选填。
3. Settings → Pages → Source 选择 GitHub Actions。
4. 运行 Daily Macro & CapEx Intelligence Auto-Update。

工作流每天北京时间08:00触发，GitHub可能延迟执行；更新JSON并提交，然后显式发布Pages。
这可避免 GITHUB_TOKEN 自动提交不触发分支式 Pages 构建的问题。
发布包仅包含 index.html 与 data/macro_daily.json，不包含本地依赖与配置文件。

前端依赖 React、Babel、Tailwind 公共 CDN，需要网络访问这些服务。

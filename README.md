# etf-momentum-screener
小资金支付宝场外基金与场内 ETF 双时段动量筛选和微信推送系统

## 修改资金规模

在私有仓库进入 `Settings -> Secrets and variables -> Actions -> Variables`，添加或修改：

- `TOTAL_CAPITAL`：总资金，默认 `2000`
- `BUY_MIN`：单笔最小金额，默认 `100`
- `BUY_MAX`：单笔最大金额，默认 `250`

例如计划投入 1000 元，可设置 `TOTAL_CAPITAL=1000`。本地运行时也可以使用：

```bash
TOTAL_CAPITAL=1000 python etf_workbench.py
```

系统会在线核验核心场外基金的代码与名称，报告每个标的的最新数据日期；数据超过 10 个自然日时停止给出交易信号。行情接口、基金净值发布时间和网络状态仍可能导致延迟，输出仅供研究参考。

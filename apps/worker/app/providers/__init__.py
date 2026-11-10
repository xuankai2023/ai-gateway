# Providers Python 版 Provider 接口（行为契约与 Go 版一致）
# 实现 ChatCompletion / Embedding / Moderation 三方法
# 目的：Worker 不用通过 Gateway 调模型，直接走 Provider 省一跳

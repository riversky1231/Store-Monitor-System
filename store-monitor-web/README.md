# Amazon Store Monitor

亚马逊店铺上新监控桌面应用（Windows 单文件 EXE + FastAPI Web 控制台）。

应用以系统托盘方式在后台运行，由用户手动触发商品抓取并发送邮件报告，支持任务管理、健康状态监控、批量导入、分页与搜索。

## 核心功能

- 后台运行：`AmazonStoreMonitor.exe` 无控制台窗口，托盘可打开管理页/日志/退出
- 首次引导：`/setup` 初始化管理员密码与 SMTP 配置
- 任务管理：新增、编辑、暂停/启用、立即运行、删除
- 批量导入：支持从旧版 `.db` 文件导入任务（自动跳过重复 URL）
- 任务页增强：
  - 分页（每页 10 条）
  - 按任务名称搜索
  - 执行中状态（旋转图标 + `检索中...`）
  - 页面自动刷新（默认 15 秒，编辑/弹窗时不打断）
- 抓取执行模型：全局串行队列（同一时刻只跑一个任务）
- 通知邮件：
  - 新上架/下架变更邮件
  - 首次抓取成功后发送初始摘要邮件
  - 连续空抓取告警邮件 + 恢复通知邮件
- 健康状态：`healthy / warning / alert` 可视化
- 数据维护：商品状态和发送过程写入日志，历史数据不再按保留天数自动删除
- 安全能力：
  - Basic Auth
  - 失败限流（默认 5 次/60 秒）
  - 非 localhost 默认要求 HTTPS
  - URL 安全校验（阻止私网/回环地址）

## 运行方式

### 方式 1：直接运行 EXE（推荐）

在 `dist` 目录运行：

```powershell
.\AmazonStoreMonitor.exe
```

启动后会自动打开：`http://127.0.0.1:8000`

### 方式 2：源码运行（开发）

```powershell
cd store-monitor-web
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
python app.py
```

## 首次初始化

1. 打开 `http://127.0.0.1:8000/setup`
2. 设置管理员密码（至少 6 位）
3. 设置 SMTP：
   - SMTP 服务器（如 `smtp.qq.com`）
   - 端口（如 `465`）
   - 发件邮箱
   - 邮箱授权码（不是登录密码）

## 任务页操作说明

- 新增任务：填写任务名称、目标 URL、CSS 选择器、通知邮箱
- 导入任务：上传 `.db` 文件，系统自动校验并导入
- 搜索任务：按任务名称模糊匹配
- 分页浏览：每页 10 条
- 单任务运行：运行完成后发送一封该任务报告
- 分组运行：该分组全部完成后发送一封分组报告
- 全部运行：按分组分别发送报告，每个分组一封
- 检索中状态：任务执行时“执行状态”列显示旋转图标
- 系统不会自动定时抓取或发送日报，所有运行均由用户手动发起

## 邮件通知逻辑

- 每次手动运行完成都会发送报告，包括无变化和运行失败的情况
- 报告列出当前抓取到的全部商品链接，不截断为前 50 条
- 分组报告仅发送给该分组任务配置的收件人；“全部运行”不会合并成一封长邮件
- 如果本轮出现待人工确认任务，先发送一封只列待确认店铺和原因的提醒邮件；正常店铺及商品内容不会出现在提醒中
- 本轮完整分组报告会暂存在数据库中，待该批次所有人工确认或重试完成后再统一发送
- 邮件发送失败时，报告会先保存在数据库发件箱中；程序启动或下次手动运行时自动补发，并在分组页显示待补发状态

## 数据文件

默认持久化目录（Windows）：`%APPDATA%\AmazonStoreMonitor`

- `monitor.db`：SQLite 主数据库
- `.store_monitor_secret.key`：SMTP 密码加密密钥（请妥善备份）
- `logs\monitor.log`：运行日志

可通过环境变量 `MONITOR_WEB_DATA_DIR` 自定义目录。

## 环境变量

### Web 鉴权

- `MONITOR_WEB_DISABLE_AUTH`：关闭鉴权（`1/true/yes`）
- `MONITOR_WEB_USERNAME`：管理员用户名（默认 `admin`）
- `MONITOR_WEB_PASSWORD`：管理员密码（设置后可跳过 DB 中密码）
- `MONITOR_WEB_REQUIRE_HTTPS`：非 localhost 是否强制 HTTPS（默认开启）
- `MONITOR_WEB_AUTH_MAX_ATTEMPTS`：限流最大失败次数（默认 `5`）
- `MONITOR_WEB_AUTH_WINDOW_SECONDS`：限流窗口秒数（默认 `60`）

### SMTP 与保留策略

- `STORE_MONITOR_SMTP_PASSWORD`：覆盖数据库中的 SMTP 授权码
- `STORE_MONITOR_SECRET_KEY`：直接指定 Fernet 密钥
- `STORE_MONITOR_SECRET_FILE`：指定密钥文件路径
- `STORE_MONITOR_RETENTION_DAYS`：覆盖数据库中的保留天数

## 测试

```powershell
# unittest 风格测试
python -m unittest discover -s tests -v

# pytest 风格测试
pytest -q tests/
```

## 构建 EXE

```powershell
cd store-monitor-web
build.bat
```

输出：

- `dist\AmazonStoreMonitor.exe`
- （可选）`build.bat onedir` 输出 `dist\AmazonStoreMonitor\AmazonStoreMonitor.exe`

## 故障排查

- 收不到邮件：
  - 检查邮箱授权码是否正确
  - 检查 SMTP 服务器/端口
  - 查看日志中 `Failed to send email` 报错
- 任务无法添加：
  - 检查 URL 与邮箱格式
  - 确认目标站点可访问
- 页面状态不刷新：
  - 任务页默认每 15 秒自动刷新
  - 输入表单或弹窗打开时自动刷新会暂停

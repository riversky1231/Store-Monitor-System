import time
import json
import os
import smtplib
import sys
from email.mime.text import MIMEText
import requests
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ================= 配置读取 =================
# 优先从环境变量读取；也可在此处直接填写（不推荐提交到版本控制）
TARGET_URL           = os.getenv("TARGET_URL", "")
CHECK_INTERVAL_HOURS = int(os.getenv("CHECK_INTERVAL_HOURS", "6"))
DATA_FILE            = os.getenv("DATA_FILE", "previous_products.json")
SMTP_SERVER          = os.getenv("SMTP_SERVER", "smtp.qq.com")
SMTP_PORT            = int(os.getenv("SMTP_PORT", "465"))
SENDER_EMAIL         = os.getenv("SENDER_EMAIL", "")
SENDER_PASSWORD      = os.getenv("SENDER_PASSWORD", "")   # 邮箱应用授权码，非登录密码
RECEIVER_EMAIL       = os.getenv("RECEIVER_EMAIL", "")
# ============================================

_REQUIRED = {
    "TARGET_URL": TARGET_URL,
    "SENDER_EMAIL": SENDER_EMAIL,
    "SENDER_PASSWORD": SENDER_PASSWORD,
    "RECEIVER_EMAIL": RECEIVER_EMAIL,
}
_missing = [k for k, v in _REQUIRED.items() if not v]
if _missing:
    logging.error("以下必填配置缺失，请通过环境变量或直接修改脚本顶部变量提供: %s", ", ".join(_missing))
    sys.exit(1)

def fetch_products():
    """访问网页并抓取商品标题和链接"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        logging.info(f"正在访问 URL: {TARGET_URL}")
        # 请求网页
        response = requests.get(TARGET_URL, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        products = []
        # ⚠️ 注意：这里的选择器需要你根据目标网站的源码(F12)进行修改！
        # 这里以常见的 div class="product-item" 为例
        items = soup.find_all('div', class_='product-item') 
        
        for item in items:
            name_tag = item.find('a') # 寻找包含商品名称和链接的<a>标签
            if name_tag:
                name = name_tag.text.strip()
                link = name_tag.get('href', '')
                
                # 处理相对路径链接
                if link.startswith('/'):
                    link = urljoin(TARGET_URL, link)
                
                if name and link:
                    products.append({
                        "id": link,  # 通常用链接或商品SKU作为唯一标识
                        "name": name,
                        "link": link
                    })
        logging.info(f"成功抓取到 {len(products)} 个商品")
        return products
    except Exception as e:
        logging.error(f"抓取网页失败: {e}")
        return []

def compare_data(current_products_list):
    """与上一次的数据进行比对"""
    # 1. 读取历史数据
    previous_products = {}
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                previous_products = json.load(f)
        except Exception as e:
            logging.error(f"读取历史数据失败: {e}")
            
    # 2. 寻找新品
    new_products = []
    current_products_dict = {}
    
    for product in current_products_list:
        pid = product['id']
        current_products_dict[pid] = product
        
        # 如果这个商品ID不在历史记录中，说明是本次上新的
        if pid not in previous_products:
            new_products.append(product)
            
    # 3. 保存本次数据，供下次比对使用
    # 只有在成功获取到数据时才覆盖，防止网络波动导致旧数据丢失被误报
    if current_products_list:
        try:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(current_products_dict, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logging.error(f"保存最新数据失败: {e}")
        
    return new_products

def send_email(new_products):
    """将新品发送到邮箱"""
    if not new_products:
        return
        
    subject = f"🔔 监控提醒：发现 {len(new_products)} 款竞争对手上新商品！"
    
    body_lines = ["发现以下新品上架：\n"]
    for i, p in enumerate(new_products, 1):
        body_lines.append(f"{i}. {p['name']}\n   链接: {p['link']}\n")
        
    body = "\n".join(body_lines)
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL
    
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        logging.info(f"✅ 成功发送邮件，包含 {len(new_products)} 个新品。")
    except Exception as e:
        logging.error(f"❌ 发送邮件失败: {e}")

def run_workflow():
    logging.info("开始执行监控工作流...")
    
    # 第 1 步：抓取网页
    current_products = fetch_products()
    if not current_products:
        logging.warning("未获取到商品，结束本次流程。")
        return
        
    # 第 2 步：比对数据 (包含保存最新数据功能)
    new_products = compare_data(current_products)
    
    # 第 3 步：发邮件
    if new_products:
        logging.info(f"🎉 发现 {len(new_products)} 个新商品！正在触发邮件通知...")
        send_email(new_products)
    else:
        logging.info("💤 本次未发现新商品。")

if __name__ == "__main__":
    logging.info(f"🚀 监控系统已启动！触发器：每 {CHECK_INTERVAL_HOURS} 小时运行一次。")
    
    # 启动时先跑一次
    run_workflow()
    
    # 触发器循环
    while True:
        logging.info(f"等待 {CHECK_INTERVAL_HOURS} 小时后进行下一次检查...")
        time.sleep(CHECK_INTERVAL_HOURS * 3600)
        run_workflow()

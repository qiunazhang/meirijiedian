import requests
import base64
import os
import json
import re
from datetime import datetime
from urllib.parse import quote, urlsplit, parse_qsl
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= ⚙️ 核心配置区 ⚙️ =================

CUSTOM_REMARK_B64 = "56eR5oqA5YWx5LqrLeW8gOa6kOiKgueCuQ=="

# 2. 节点订阅源库（随时可在末尾追加新链接）
SOURCE_URLS = [
    "https://cdn.jsdelivr.net/gh/Pawdroid/Free-servers@main/sub",
    "https://cdn.jsdelivr.net/gh/mfuu/v2ray@master/v2ray",
    "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt",
    "https://raw.githubusercontent.com/free-nodes/v2rayfree/main/v202605312",
    "https://raw.githubusercontent.com/chengaopan/AutoMergePublicNodes/master/list.txt",
    "https://github.cmliussss.net/https://raw.githubusercontent.com/qmqv/jd07/refs/heads/main/v207-1010.txt",
    "https://ghfast.top/https://raw.githubusercontent.com/free18/v2ray/refs/heads/main/v.txt",
    "https://proxy.v2gh.com/https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub",
    "https://raw.githubusercontent.com/ts-sf/fly/main/v2",
    "https://sub.proxygo.org/v2ray.php?key=191c91f624a800e83942463fd667bba5",
    "https://raw.githubusercontent.com/mahdibland/ShadowsocksAggregator/master/Eternity",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/V2RAY_BASE64.txt",
    "https://app.sublink.works/x/ZrVEXNV",
    "https://gcore.jsdelivr.net/gh/aews/jd2/v20528.txt",
    "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/splitted/vmess.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/V2Ray-Config-By-EbraSha-All-Type.txt",
    "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/sub/splitted/trojan.txt",
    "https://gt.1155555.xyz/https://raw.githubusercontent.com/shaoyouvip/free/refs/heads/main/base64.txt",
]

# 3. 垃圾节点过滤黑名单（可自行添加别人节点里的广告词过滤掉）
BLACKLIST_KEYWORDS = ['-1', '127.0.0.1', 'timeout', 'err', '错误', '剩余', '到期', '官网', 'mibei77', '别买']

# 并发抓取线程数
MAX_WORKERS = 16

# 最终保留的最大节点数（去重后截取，控制订阅体积）
MAX_NODES = 3000

# 协议优先级：数字越小越优先保留（较新/性能较好的协议靠前）
# 优先级：hysteria2 > vless > vmess > 其余
PROTOCOL_PRIORITY = {
    'hysteria2://': 0, 'vless://': 1, 'vmess://': 2,
    'tuic://': 3, 'trojan://': 4, 'ss://': 5, 'ssr://': 6,
}

# ====================================================

SUPPORTED_PROTOCOLS = ('vmess://', 'vless://', 'ss://', 'ssr://', 'trojan://', 'tuic://', 'hysteria2://')


def try_base64_decode(content):
    """仅当内容看起来是 base64（不含协议头、字符集合法）时才解码，避免误判明文订阅。"""
    sample = content[:512]
    if any(p in sample for p in SUPPORTED_PROTOCOLS):
        return None  # 已是明文，不要解码
    if not re.fullmatch(r'[A-Za-z0-9+/=\s]+', content):
        return None
    try:
        padded = content + "=" * (-len(content) % 4)
        decoded = base64.b64decode(padded).decode('utf-8', errors='ignore')
        # 解码后必须能看到协议头才算成功
        if any(p in decoded for p in SUPPORTED_PROTOCOLS):
            return decoded
    except Exception:
        pass
    return None


def fetch_and_decode(url):
    if not url or not url.strip():  # 过滤掉不小心留空的链接
        return []
    try:
        print(f"[*] 正在抓取: {url}")
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        content = response.text.strip()
        if not content:
            return []

        decoded = try_base64_decode(content)
        return (decoded or content).splitlines()
    except Exception as e:
        print(f"[!] 抓取失败 {url}: {e}")
        return []


def node_identity(link):
    """按 IP:port 去重，同一服务器不同端口都保留。"""
    if link.startswith("vmess://"):
        try:
            b64 = link[8:]
            v = json.loads(base64.b64decode(b64 + "=" * (-len(b64) % 4)).decode('utf-8'))
            return f"{v.get('add')}:{v.get('port')}".lower()
        except Exception:
            return link
    # vless/trojan/ss/ssr/tuic/hysteria2：取 host:port（去掉用户信息部分）
    try:
        netloc = urlsplit(link.split("#", 1)[0]).netloc.lower()
        host_port = netloc.rsplit("@", 1)[-1]   # 去掉 user:pass@ 前缀
        return host_port
    except Exception:
        return link.split("#", 1)[0]


def rename_node(link, index):
    try:
        custom_remark = base64.b64decode(CUSTOM_REMARK_B64).decode('utf-8')
    except Exception:
        custom_remark = "Node"  # 万一解码失败的保底名字

    new_name = f"{custom_remark} {index:03d}"

    if link.startswith("vmess://"):
        try:
            b64_str = link[8:]
            b64_str += "=" * (-len(b64_str) % 4)
            v_json = json.loads(base64.b64decode(b64_str).decode('utf-8'))
            v_json['ps'] = new_name  # 强行覆盖 VMess 别名
            new_b64 = base64.b64encode(
                json.dumps(v_json, ensure_ascii=False).encode('utf-8')).decode('utf-8')
            return f"vmess://{new_b64}"
        except Exception:
            return link

    elif link.startswith(('vless://', 'trojan://', 'ss://', 'ssr://', 'tuic://', 'hysteria2://')):
        # 兼容处理：防范原本就不带 # 备注符号的特殊链接
        base_link = link.split("#", 1)[0]
        return f"{base_link}#{quote(new_name)}"

    return link


def main():
    print(f"=== 开始执行高级抓取与清洗任务 {datetime.now()} ===")
    all_lines = []

    # 并发抓取，保留源顺序
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_and_decode, url): i for i, url in enumerate(SOURCE_URLS)}
        results = [None] * len(SOURCE_URLS)
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()
    for r in results:
        all_lines.extend(r or [])

    # 清洗 + 保序去重（按真实地址）
    seen = set()
    valid_nodes = []
    for line in all_lines:
        line = line.strip()
        if not line.startswith(SUPPORTED_PROTOCOLS):
            continue
        if any(kw.lower() in line.lower() for kw in BLACKLIST_KEYWORDS):
            continue
        if not line.startswith('hysteria2://'):   # 只保留 hysteria2 节点
            continue
        ident = node_identity(line)
        if ident in seen:
            continue
        seen.add(ident)
        valid_nodes.append(line)

    print(f"[*] 去重后共 {len(valid_nodes)} 个 hysteria2 节点，不限数量全部保留...")

    # 稳定排序减少 Git diff 抖动（全是 hysteria2，按地址排）
    valid_nodes.sort()

    print(f"[*] 正在进行全自动重命名...")
    final_nodes = [rename_node(node, i) for i, node in enumerate(valid_nodes, 1)]

    print(f"[*] 成功生成 {len(final_nodes)} 个定制化节点！")

    raw_text = "\n".join(final_nodes)
    sub_base64 = base64.b64encode(raw_text.encode('utf-8')).decode('utf-8')

    os.makedirs('output', exist_ok=True)
    with open('output/nodes.txt', 'w', encoding='utf-8') as f:
        f.write(raw_text)
    with open('output/sub.txt', 'w', encoding='utf-8') as f:
        f.write(sub_base64)


if __name__ == "__main__":
    main()

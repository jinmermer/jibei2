#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
国网ECP「冀北」招标公告 + 采购公告 监控 + 钉钉推送

抓取口径（重要）：
  用关键字 key=冀北 检索「招标公告及投标邀请书」和「采购公告」两个栏目。
  为什么不按"招标单位=冀北"？
    该下拉只匹配冀北本部（orgId=2019061900137008），完全漏掉其 35 家下属单位：
    唐山/承德/秦皇岛/张家口供电公司、超高压分公司、物资分公司、工程管理分公司、
    电科院、智能配电网中心、风光储输、思极科技、北京送变电等。
    实测：orgId 口径 425 条，关键字口径 611 条，多出 201 条下属单位公告。
  二次校验：结果再按「标题或发布单位含"冀北"」过滤，实测 0 条误入，无需额外剔除。

输出：
  data/jb_notices.json     全量库（去重依据）
  data/jb_YYYYMMDD.md      当日新增（日期 | 标题 表格）
  钉钉群机器人推送

用法：
  python jb_notices.py                       # 抓取 + 生成日报（未配 webhook 则只打印）
  python jb_notices.py --baseline            # 只建基线不推送
  DING_WEBHOOK=... DING_SECRET=... python jb_notices.py
"""
import asyncio, json, sys, os, time, base64, hashlib, hmac, urllib.parse, datetime
from pathlib import Path

BASE = "https://ecp.sgcc.com.cn/ecp2.0"
ENTRY_URL = f"{BASE}/portal/#/list/list-com/2018032600000014_5_2018032700291334"
API = f"{BASE}/ecpwcmcore//index/noteList"

KEYWORD = "冀北"
MAX_PAGES = 30            # 每栏目最多翻 30 页 × 100 条，远超实际条数
PAGE_SIZE = 100

COLUMNS = [
    {"key": "zb", "name": "招标公告及投标邀请书", "menu": "2018032700291334"},
    {"key": "cg", "name": "采购公告",             "menu": "2018032900295987"},
]

# 电工交易专区公告（独立栏目、独立接口，主站"招标采购"菜单下搜不到）
ETP_API = f"{BASE}/ecpwcmcore//index/etpNoticeList"
ETP_DOC_URL = "https://sgccetp.com.cn/portal/#/doc/"
ETP_COLUMNS = [
    {"key": "etp_zb", "name": "电工专区·招标公告", "menu": "2020110706504025"},
    {"key": "etp_cg", "name": "电工专区·采购公告", "menu": "2020110706504027"},
]

ROOT = Path(__file__).parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)
STORE = DATA / "jb_notices.json"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


async def fetch_column(page, menu):
    """在页面上下文内调接口翻页（借浏览器带上瑞数动态签名）"""
    rows, total = [], None
    for idx in range(1, MAX_PAGES + 1):
        body = {
            "index": idx, "size": PAGE_SIZE, "firstPageMenuId": menu,
            "purOrgStatus": "", "purOrgCode": "", "purType": "", "noticeType": "",
            "orgId": "", "key": KEYWORD, "orgName": "",
        }
        raw = await page.evaluate(
            """async (a) => {
                const r = await fetch(a.url, {method:'POST',
                    headers:{'Content-Type':'application/json;charset=UTF-8'},
                    body: JSON.stringify(a.body)});
                return await r.text();
            }""", {"url": API, "body": body})
        try:
            rv = json.loads(raw).get("resultValue") or {}
        except Exception:
            print(f"[warn] menu={menu} 第{idx}页解析失败", file=sys.stderr)
            break
        total = rv.get("count", total)
        lst = rv.get("noteList") or []
        if not lst:
            break
        rows.extend(lst)
        if total and len(rows) >= total:
            break
        await page.wait_for_timeout(250)
    return rows, total


def normalize(r, col_key):
    did = str(r.get("firstPageDocId") or r.get("id") or "")
    title = (r.get("title") or "").strip()
    return {
        "id": did,
        "type": col_key,                       # zb=招标公告, cg=采购公告
        "date": (r.get("noticePublishTime") or "")[:10],
        "org": (r.get("publishOrgName") or "").strip(),
        "title": title,
        "url": f"{BASE}/portal/#/doc/doc-com/{did}" if did else "",
    }


async def fetch_etp(page, menu, max_pages=20):
    """电工交易专区：独立接口 etpNoticeList"""
    rows, total = [], None
    for idx in range(1, max_pages + 1):
        body = {"index": idx, "size": 100, "noticeMenuId": menu, "key": KEYWORD}
        raw = await page.evaluate(
            """async (a) => {
                const r = await fetch(a.url, {method:'POST',
                    headers:{'Content-Type':'application/json;charset=UTF-8'},
                    body: JSON.stringify(a.body)});
                return await r.text();
            }""", {"url": ETP_API, "body": body})
        try:
            rv = json.loads(raw).get("resultValue") or {}
        except Exception:
            print(f"[warn] etp menu={menu} 第{idx}页解析失败", file=sys.stderr)
            break
        total = rv.get("count", total)
        lst = rv.get("noticeList") or []
        if not lst:
            break
        rows.extend(lst)
        if total and len(rows) >= total:
            break
        await page.wait_for_timeout(250)
    return rows, total


def normalize_etp(r, col_key):
    """电工专区字段：title/orgName/noticePublishTime/url(相对路径)"""
    rel = r.get("url") or ""
    did = str(r.get("noticeId") or r.get("id1") or rel)
    return {
        "id": f"etp_{did}",
        "type": col_key,
        "date": (r.get("noticePublishTime") or "")[:10],
        "org": (r.get("orgName") or "").strip(),
        "title": (r.get("title") or "").strip(),
        "url": ETP_DOC_URL + rel if rel else "",
    }


def is_jibei(item):
    """二次校验：标题或发布单位需含"冀北"（实测无误入，作为保险）"""
    return "冀北" in item["title"] or "冀北" in item["org"]


async def fetch_all():
    from playwright.async_api import async_playwright
    result = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = await browser.new_context(
            ignore_https_errors=True, locale="zh-CN", user_agent=UA)
        page = await ctx.new_page()
        await page.goto(ENTRY_URL, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(9000)      # 等瑞数脚本注入
        for col in COLUMNS:
            rows, total = await fetch_column(page, col["menu"])
            items = [normalize(r, col["key"]) for r in rows]
            items = [x for x in items if x["id"] and is_jibei(x)]
            result[col["key"]] = items
            print(f"[info] {col['name']}：接口 {total} 条，抓取 {len(rows)} 条，"
                  f"有效 {len(items)} 条", file=sys.stderr)
            await page.wait_for_timeout(400)
        # 电工交易专区（独立接口 etpNoticeList）
        for col in ETP_COLUMNS:
            rows, total = await fetch_etp(page, col["menu"])
            items = [normalize_etp(r, col["key"]) for r in rows]
            items = [x for x in items if x["id"] and is_jibei(x)]
            result[col["key"]] = items
            print(f"[info] {col['name']}：接口 {total} 条，抓取 {len(rows)} 条，"
                  f"有效 {len(items)} 条", file=sys.stderr)
            await page.wait_for_timeout(400)
        await browser.close()
    return result


def load_store():
    if STORE.exists():
        try:
            return json.loads(STORE.read_text("utf-8"))
        except Exception:
            return {}
    return {}


def build_md(new_by_col, scanned, limit_per_col=None):
    """生成钉钉 Markdown。标题即链接，点开直达公告原文。"""
    today = datetime.date.today().isoformat()
    lines = [f"### 国网ECP · 冀北招标&采购公告", "",
             f"**{today}** 更新", ""]
    total = 0
    for col in COLUMNS + ETP_COLUMNS:
        items = new_by_col[col["key"]]
        total += len(items)
        lines.append(f"**{col['name']}**（新增 {len(items)} 条）")
        lines.append("")
        if not items:
            lines.append("　今日无新增")
            lines.append("")
            continue
        shown = items if not limit_per_col else items[:limit_per_col]
        lines.append("| 发布日期 | 公告标题（点击查看原文） |")
        lines.append("|---|---|")
        for r in shown:
            t = r["title"].replace("|", "｜")
            lines.append(f"| {r['date']} | [{t}]({r['url']}) |")
        if len(items) > len(shown):
            lines.append(f"| 　 | …… 另有 {len(items)-len(shown)} 条，见附件「完整清单」 |")
        lines.append("")
    lines.append(f"---")
    scan_txt = "、".join(f"{col['name']} {scanned[col['key']]}" for col in COLUMNS + ETP_COLUMNS if col["key"] in scanned)
    lines.append(f"合计新增 **{total}** 条｜扫描范围：{scan_txt}")
    return "\n".join(lines) + "\n"


def ding_sign(secret, ts):
    s = f"{ts}\n{secret}"
    h = hmac.new(secret.encode(), s.encode(), hashlib.sha256).digest()
    return urllib.parse.quote_plus(base64.b64encode(h))


def env_verify():
    """沙箱/代理环境下可设 DING_VERIFY_SSL=0 跳过证书校验；正常环境保持 True"""
    return os.getenv("DING_VERIFY_SSL", "1") not in ("0", "false", "False")


def push_dingtalk(text, title="冀北招标采购日报"):
    webhook = os.getenv("DING_WEBHOOK", "").strip()
    secret = os.getenv("DING_SECRET", "").strip()
    if not webhook:
        print("[info] 未配置 DING_WEBHOOK，跳过推送", file=sys.stderr)
        return False
    import requests, urllib3
    urllib3.disable_warnings()
    if secret:
        ts = str(round(time.time() * 1000))
        webhook = f"{webhook}&timestamp={ts}&sign={ding_sign(secret, ts)}"
    payload = {"msgtype": "markdown",
               "markdown": {"title": title, "text": text}}
    # 先按标准校验证书发送；若是代理/内网证书导致失败，自动降级重试一次
    modes = [env_verify()]
    if modes[0]:
        modes.append(False)
    resp = None
    for verify in modes:
        try:
            resp = requests.post(webhook, json=payload, timeout=30, verify=verify)
            break
        except requests.exceptions.SSLError as e:
            print(f"[warn] SSL 校验失败(verify={verify})，{'降级重试' if verify else '放弃'}", file=sys.stderr)
            if not verify:
                raise
    ok = resp.ok and '"errcode":0' in resp.text.replace(" ", "")
    print(f"[info] 推送{'成功' if ok else '失败'}", file=sys.stderr)
    return ok


def main():
    first_run = not STORE.exists()          # 首次运行：只建库，不推送历史
    baseline_only = "--baseline" in sys.argv or first_run
    fetched = asyncio.run(fetch_all())

    store = load_store()
    new_by_col, scanned = {}, {}
    for col in COLUMNS + ETP_COLUMNS:
        items = fetched[col["key"]]
        scanned[col["key"]] = len(items)
        fresh = [x for x in items if x["id"] not in store]
        fresh.sort(key=lambda x: x["date"], reverse=True)
        new_by_col[col["key"]] = [] if baseline_only else fresh
        for x in items:
            store[x["id"]] = x
    STORE.write_text(json.dumps(store, ensure_ascii=False, indent=1), "utf-8")

    md = build_md(new_by_col, scanned, limit_per_col=15)
    (DATA / f"jb_{datetime.date.today():%Y%m%d}.md").write_text(md, "utf-8")
    print(md)
    from collections import Counter
    tc = Counter(v["type"] for v in store.values())
    print(f"[info] 库内共 {len(store)} 条（{dict(tc)}）", file=sys.stderr)

    if "--baseline" in sys.argv:
        print("[info] baseline 模式，不推送", file=sys.stderr)
        return

    if first_run:
        from collections import Counter
        tc = Counter(v["type"] for v in store.values())
        msg = ("### 冀北公告监控 · 初始化完成\n\n"
               f"已建立基线库，共收录 **{len(store)}** 条历史公告\n\n"
               f"- 主站招标 {tc.get('zb',0)} / 主站采购 {tc.get('cg',0)}\n"
               f"- 电工专区招标 {tc.get('etp_zb',0)} / 电工专区采购 {tc.get('etp_cg',0)}\n\n"
               "从明天起，只会推送**新增**的公告。")
        push_dingtalk(msg, "冀北公告监控-初始化")
        print("[info] 首次运行，已建基线并推送初始化说明", file=sys.stderr)
        return

    total_new = sum(len(v) for v in new_by_col.values())
    # 静默模式：无新增时不打扰（设 DING_ONLY_WHEN_NEW=1 启用）
    if os.getenv("DING_ONLY_WHEN_NEW", "0") in ("1", "true", "True") and total_new == 0:
        print("[info] 无新增，跳过推送（静默模式）", file=sys.stderr)
        return
    push_dingtalk(md)


if __name__ == "__main__":
    main()

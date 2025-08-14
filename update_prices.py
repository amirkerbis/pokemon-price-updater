import os, time, datetime, requests
from typing import List, Dict, Any, Tuple
from supabase import create_client, Client

# -------- Env & config --------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_KEY"]
POKEMON_TCG_API_KEY = os.environ["POKEMON_TCG_API_KEY"]

PAGE_SIZES_TRY      = [int(x) for x in os.getenv("PAGE_SIZES", "100,50,25").split(",")]
BETWEEN_PAGES_DELAY = float(os.getenv("BETWEEN_PAGES_DELAY", "1.0"))
POST_BATCH_DELAY    = float(os.getenv("POST_BATCH_DELAY", "1.0"))
MAX_RETRIES         = int(os.getenv("MAX_RETRIES", "4"))
REQ_TIMEOUT         = int(os.getenv("REQ_TIMEOUT", "60"))

TODAY = datetime.date.today().isoformat()

# -------- Clients --------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CARDS_URL = "https://api.pokemontcg.io/v2/cards"
SETS_URL  = "https://api.pokemontcg.io/v2/sets"
SESSION = requests.Session()
SESSION.headers.update({
    "X-Api-Key": POKEMON_TCG_API_KEY,
    "Accept": "application/json",
    "User-Agent": "PokemonPriceTracker/1.0 (+github-actions)"
})

# -------- Helpers --------
def rows_from_card(card: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    tcg = card.get("tcgplayer") or {}
    prices = tcg.get("prices") or {}
    for variant, pdata in prices.items():
        if isinstance(pdata, dict):
            out.append({
                "card_id": card.get("id"),
                "variant": variant,      # normal / holofoil / reverseHolofoil / firstEdition*
                "date": TODAY,
                "market": pdata.get("market"),
                "low": pdata.get("low"),
                "high": pdata.get("high"),
            })
    return out

def upsert_prices(rows: List[Dict[str, Any]]):
    if not rows:
        return
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            supabase.table("card_prices").upsert(
                rows,
                on_conflict="card_id,variant,date"
            ).execute()
            time.sleep(POST_BATCH_DELAY)
            return
        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"❌ upsert failed ({len(rows)} rows): {e}")
                return
            sleep_s = 2 ** attempt
            print(f"⏳ upsert retry {attempt}/{MAX_RETRIES} -> wait {sleep_s}s | {e}")
            time.sleep(sleep_s)

def set_exists_in_api(set_id: str) -> bool:
    """
    נחזיר False רק אם הוכחנו בוודאות שהסט לא קיים:
    - 3 ניסיונות ל-GET /v2/sets/{id} עם backoff קצר
    - אם עדיין 404 → בדיקת חיפוש /v2/sets?q=id:{id}
    אחרת נחזיר True (כדי לא לסמן skip בטעות)
    """
    for attempt in range(1, 4):
        try:
            r = SESSION.get(f"{SETS_URL}/{set_id}", timeout=REQ_TIMEOUT)
            if r.status_code == 200:
                return True
            if r.status_code == 404:
                break
        except Exception:
            pass
        time.sleep(0.5 * attempt)

    try:
        r2 = SESSION.get(
            SETS_URL,
            params={"q": f"id:{set_id}", "select": "id", "pageSize": 1},
            timeout=REQ_TIMEOUT,
        )
        if r2.status_code == 200:
            return bool((r2.json() or {}).get("data") or [])
        return True
    except Exception:
        return True

def fetch_cards_page(set_id: str, page: int) -> Tuple[List[Dict[str, Any]], str]:
    """
    מחזיר (cards, status):
      "ok"    – קיבלנו דף (יכול להיות ריק = סוף הסט)
      "retry" – כשל זמני (ננסה בהרצה הבאה את אותו דף)
      "skip"  – רק אם הוכח שהסט לא קיים ב-API
    """
    for size in PAGE_SIZES_TRY:
        params = {
            "q": f"set.id:{set_id}",
            "page": page,
            "pageSize": size,
            "orderBy": "id",
            "select": "id,tcgplayer",
        }
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = SESSION.get(CARDS_URL, params=params, timeout=REQ_TIMEOUT)
                if r.status_code == 200:
                    return r.json().get("data", []), "ok"
                if r.status_code in (429, 500, 502, 503, 504):
                    sleep_s = 2 ** attempt
                    print(f"⏳ set {set_id} page {page} size {size}: HTTP {r.status_code} -> wait {sleep_s}s")
                    time.sleep(sleep_s)
                    continue
                if r.status_code == 404:
                    if not set_exists_in_api(set_id):
                        print(f"⚠️ set {set_id}: confirmed not in /v2/sets → skipping this set")
                        return [], "skip"
                    print(f"⏳ set {set_id} page {page} size {size}: HTTP 404 on cards, set exists → retry later")
                    return [], "retry"
                print(f"⏳ set {set_id} page {page} size {size}: HTTP {r.status_code} → temporary")
                return [], "retry"
            except requests.Timeout:
                sleep_s = 2 ** attempt
                print(f"⏳ set {set_id} page {page} size {size}: timeout -> wait {sleep_s}s")
                time.sleep(sleep_s)
            except Exception as e:
                sleep_s = 2 ** attempt
                print(f"⏳ set {set_id} page {page} size {size}: error {e} -> wait {sleep_s}s")
                time.sleep(sleep_s)
        print(f"↘️  set {set_id} page {page}: falling back from size {size}")
    return [], "retry"

def get_progress(set_id: str) -> Dict[str, Any]:
    data = supabase.table("price_run_progress") \
        .select("last_page_done, done") \
        .eq("run_date", TODAY).eq("set_id", set_id).execute().data
    if data:
        return {"last_page_done": data[0]["last_page_done"], "done": data[0]["done"]}
    supabase.table("price_run_progress").upsert({
        "run_date": TODAY, "set_id": set_id, "last_page_done": 0, "done": False
    }, on_conflict="run_date,set_id").execute()
    return {"last_page_done": 0, "done": False}

def update_progress(set_id: str, page: int = None, done: bool = None):
    patch = {"run_date": TODAY, "set_id": set_id}
    if page is not None:
        patch["last_page_done"] = page
    if done is not None:
        patch["done"] = done
    supabase.table("price_run_progress").upsert(
        patch, on_conflict="run_date,set_id"
    ).execute()

def main():
    # קרא את רשימת הסטים מסופבייס
    sets = supabase.table("sets").select("id").order("id").execute().data
    set_ids = [s["id"] for s in sets]

    total_rows = 0
    total_cards_seen = 0
    sets_done: List[str] = []
    sets_skipped: List[str] = []
    sets_retry: List[str] = []

    print("🚀 מתחיל עדכון מחירים יומי (bulk paging + resume + smart 404)…")

    for set_id in set_ids:
        prog = get_progress(set_id)
        if prog["done"]:
            print(f"⏭️  set {set_id}: כבר סומן כסיום להיום — דילוג")
            continue

        page = (prog["last_page_done"] or 0) + 1
        print(f"▶️ set {set_id}: ממשיך מעמוד {page}")

        while True:
            cards_page, status = fetch_cards_page(set_id, page)

            if status == "skip":
                sets_skipped.append(set_id)
                update_progress(set_id, page=0, done=True)
                break

            if status == "retry":
                sets_retry.append(set_id)
                print(f"↩️ set {set_id} page {page}: temporary failure — will retry next run")
                break

            if not cards_page:
                sets_done.append(set_id)
                update_progress(set_id, page=page-1, done=True)
                print(f"✅ set {set_id}: הסתיים (last_page_done={page-1})")
                break

            batch_rows: List[Dict[str, Any]] = []
            for card in cards_page:
                total_cards_seen += 1
                batch_rows.extend(rows_from_card(card))

            upsert_prices(batch_rows)
            total_rows += len(batch_rows)

            update_progress(set_id, page=page, done=False)
            print(f"🟩 set {set_id} page {page}: cards={len(cards_page)} price_rows={len(batch_rows)} total_price_rows={total_rows}")

            page += 1
            time.sleep(BETWEEN_PAGES_DELAY)

    # -------- Summary --------
    print("\n================ SUMMARY ================\n")
    try:
        res_today = supabase.table("card_prices") \
            .select("id", count="exact") \
            .eq("date", TODAY) \
            .execute()
        db_today_count = getattr(res_today, "count", None)
        if db_today_count is None:
            db_today_count = len(res_today.data or [])
    except Exception as e:
        db_today_count = None
        print(f"ℹ️ לא הצלחתי להביא ספירת DB להיום: {e}")

    prog_rows = supabase.table("price_run_progress") \
        .select("set_id,last_page_done,done") \
        .eq("run_date", TODAY) \
        .execute().data or []

    done_from_db    = {r["set_id"] for r in prog_rows if r.get("done")}
    not_done_from_db= {r["set_id"] for r in prog_rows if not r.get("done")}
    remaining_sets  = [sid for sid in set_ids if sid not in done_from_db]

    def show_list(title, items, limit=25):
        items = sorted(set(items))
        print(f"{title}: {len(items)}")
        if items:
            preview = ", ".join(items[:limit])
            print("  " + preview + (" ..." if len(items) > limit else ""))

    print(f"🧾 תאריך ריצה: {TODAY}")
    print(f"⬆️  שורות מחירים שהוכנו בקוד (run-total): {total_rows}")
    if db_today_count is not None:
        print(f"📦 שורות מחירים שקיימות ב-DB להיום: {db_today_count}")

    show_list("✅ סטים שהושלמו היום (done)", sets_done or list(done_from_db))
    show_list("⏭️ סטים שדולגו (לא קיימים ב-API) (skip)", sets_skipped)
    show_list("↩️ סטים שנדחו להרצה הבאה (retry)", sets_retry or list(not_done_from_db))
    show_list("⏳ סטים שנותרו להשלמה (remaining)", remaining_sets)
    print("\n================ END SUMMARY ================\n")

if __name__ == "__main__":
    main()

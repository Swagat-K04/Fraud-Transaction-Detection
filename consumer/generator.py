"""
generator.py — Realistic synthetic credit card transaction generator.

Mirrors the original repo's dataset schema (cc_num, category, amt, merchant,
merch_lat/long, is_fraud) while adding richer behavioural signals used by
the new XGBoost feature pipeline.
"""

import random
import math
import uuid
import json
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional

# ─── Schema mirrors original repo + new fields ───────────────────────────────
@dataclass
class Customer:
    cc_num: str
    first: str
    last: str
    gender: str
    street: str
    city: str
    state: str
    zip: str
    lat: float
    long: float
    job: str
    dob: str  # YYYY-MM-DD

@dataclass
class Transaction:
    trans_num: str
    cc_num: str
    trans_time: str          # ISO-8601
    unix_time: int
    category: str
    merchant: str
    amt: float
    merch_lat: float
    merch_long: float
    # Customer denorm (for feature engineering downstream)
    cust_lat: float
    cust_long: float
    dob: str
    # Ground-truth label (used in training set only; omitted in live stream)
    is_fraud: Optional[bool] = None

# ─── Merchant catalogue ───────────────────────────────────────────────────────
CATEGORIES = {
    "grocery_pos":         {"weight": 0.20, "amt_range": (5,  300),  "fraud_multiplier": 0.4},
    "gas_transport":       {"weight": 0.12, "amt_range": (20, 150),  "fraud_multiplier": 1.2},
    "home":                {"weight": 0.09, "amt_range": (10, 800),  "fraud_multiplier": 0.6},
    "shopping_net":        {"weight": 0.14, "amt_range": (10, 2000), "fraud_multiplier": 2.1},
    "shopping_pos":        {"weight": 0.10, "amt_range": (5,  500),  "fraud_multiplier": 0.8},
    "food_dining":         {"weight": 0.11, "amt_range": (8,  200),  "fraud_multiplier": 0.5},
    "health_fitness":      {"weight": 0.05, "amt_range": (15, 300),  "fraud_multiplier": 0.3},
    "entertainment":       {"weight": 0.06, "amt_range": (5,  600),  "fraud_multiplier": 0.9},
    "travel":              {"weight": 0.05, "amt_range": (50, 5000), "fraud_multiplier": 3.0},
    "personal_care":       {"weight": 0.04, "amt_range": (5,  150),  "fraud_multiplier": 0.4},
    "kids_pets":           {"weight": 0.02, "amt_range": (10, 400),  "fraud_multiplier": 0.3},
    "misc_net":            {"weight": 0.01, "amt_range": (1,  9999), "fraud_multiplier": 5.0},
    "misc_pos":            {"weight": 0.01, "amt_range": (1,  500),  "fraud_multiplier": 1.5},
}

MERCHANTS_BY_CATEGORY = {
    "grocery_pos":    ["Whole Foods", "Trader Joe's", "Kroger", "Safeway", "Aldi"],
    "gas_transport":  ["Shell", "BP", "ExxonMobil", "Chevron", "Speedway"],
    "home":           ["Home Depot", "Lowe's", "IKEA", "Wayfair", "Bed Bath"],
    "shopping_net":   ["Amazon", "eBay", "Etsy", "Walmart.com", "Target.com"],
    "shopping_pos":   ["Walmart", "Target", "Macy's", "Best Buy", "TJ Maxx"],
    "food_dining":    ["McDonald's", "Chipotle", "Starbucks", "Panera", "Chick-fil-A"],
    "health_fitness": ["CVS Pharmacy", "Walgreens", "Planet Fitness", "GNC", "Peloton"],
    "entertainment":  ["Netflix", "Spotify", "AMC Theatres", "Steam", "Disney+"],
    "travel":         ["Delta Airlines", "United Airlines", "Marriott", "Airbnb", "Expedia"],
    "personal_care":  ["Sephora", "Ulta Beauty", "Great Clips", "Supercuts", "Bath & Body"],
    "kids_pets":      ["PetSmart", "Petco", "Toys R Us", "Carter's", "Buy Buy Baby"],
    "misc_net":       ["Unknown Merchant", "Intl Wire Transfer", "Crypto Exchange", "VPN Service"],
    "misc_pos":       ["Convenience Store", "Pawn Shop", "Money Order", "Check Cashing"],
}

US_CITIES = [
    ("New York",     "NY", 40.71, -74.00),
    ("Los Angeles",  "CA", 34.05, -118.24),
    ("Chicago",      "IL", 41.85, -87.65),
    ("Houston",      "TX", 29.76, -95.37),
    ("Phoenix",      "AZ", 33.45, -112.07),
    ("Philadelphia", "PA", 39.95, -75.17),
    ("San Antonio",  "TX", 29.42, -98.49),
    ("San Diego",    "CA", 32.72, -117.16),
    ("Dallas",       "TX", 32.79, -96.77),
    ("San Jose",     "CA", 37.34, -121.89),
]

FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer",
               "Michael", "Linda", "William", "Barbara", "David", "Susan",
               "Emma", "Liam", "Olivia", "Noah", "Ava", "Sophia", "Mason", "Lucas"]
LAST_NAMES  = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
               "Miller", "Davis", "Rodriguez", "Martinez", "Wilson", "Anderson",
               "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "White"]
JOBS = ["Software Engineer", "Nurse", "Teacher", "Accountant", "Manager",
        "Sales Rep", "Analyst", "Doctor", "Lawyer", "Electrician",
        "Plumber", "Chef", "Pilot", "Police Officer", "Architect"]


def _rand_dob(min_age: int = 22, max_age: int = 75) -> str:
    """Generate a date of birth for an age between min_age and max_age."""
    now = datetime.now()
    year = now.year - random.randint(min_age, max_age)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"


def generate_customers(n: int = 100) -> list[Customer]:
    customers = []
    used_cc = set()
    for _ in range(n):
        cc = "".join([str(random.randint(0, 9)) for _ in range(16)])
        while cc in used_cc:
            cc = "".join([str(random.randint(0, 9)) for _ in range(16)])
        used_cc.add(cc)
        city, state, lat, lng = random.choice(US_CITIES)
        customers.append(Customer(
            cc_num=cc,
            first=random.choice(FIRST_NAMES),
            last=random.choice(LAST_NAMES),
            gender=random.choice(["M", "F"]),
            street=f"{random.randint(1,9999)} {random.choice(['Main','Oak','Maple','Cedar','Pine'])} St",
            city=city, state=state,
            zip=str(random.randint(10000, 99999)),
            lat=round(lat + random.uniform(-0.5, 0.5), 6),
            long=round(lng + random.uniform(-0.5, 0.5), 6),
            job=random.choice(JOBS),
            dob=_rand_dob(),
        ))
    return customers


def _pick_category() -> str:
    cats = list(CATEGORIES.keys())
    weights = [CATEGORIES[c]["weight"] for c in cats]
    return random.choices(cats, weights=weights, k=1)[0]


def _compute_fraud_prob(cat: str, amt: float, hour: int, cust_lat: float,
                        cust_long: float, merch_lat: float, merch_long: float) -> float:
    """Heuristic fraud probability matching original dataset's ~8% base rate."""
    base = 0.05
    base *= CATEGORIES[cat]["fraud_multiplier"]
    max_amt = CATEGORIES[cat]["amt_range"][1]
    if amt > max_amt * 0.75:
        base *= 2.0
    if hour < 5 or hour > 23:
        base *= 1.8
    dist = math.sqrt((cust_lat - merch_lat)**2 + (cust_long - merch_long)**2)
    if dist > 3.0:     # ~300km+ away
        base *= 2.5
    return min(base, 0.9)


def generate_transaction(customer: Customer, label: bool = None) -> Transaction:
    """Generate one realistic transaction for a given customer."""
    cat = _pick_category()
    lo, hi = CATEGORIES[cat]["amt_range"]
    amt = round(random.uniform(lo, hi), 2)

    merchant = random.choice(MERCHANTS_BY_CATEGORY.get(cat, ["Unknown"]))
    merch_lat  = round(customer.lat  + random.uniform(-2.0, 2.0), 6)
    merch_long = round(customer.long + random.uniform(-2.0, 2.0), 6)

    now = datetime.now(timezone.utc)
    hour = now.hour

    if label is None:
        p = _compute_fraud_prob(cat, amt, hour, customer.lat, customer.long, merch_lat, merch_long)
        label = random.random() < p

    # Fraudulent transactions: skew amounts upward, odd hours, distant merchants
    if label:
        amt = round(random.uniform(lo * 0.5, hi * 1.5), 2)
        merch_lat  = round(customer.lat  + random.uniform(-5.0, 5.0), 6)
        merch_long = round(customer.long + random.uniform(-5.0, 5.0), 6)

    return Transaction(
        trans_num=str(uuid.uuid4()).replace("-", "")[:20].upper(),
        cc_num=customer.cc_num,
        trans_time=now.isoformat(),
        unix_time=int(now.timestamp()),
        category=cat,
        merchant=merchant,
        amt=amt,
        merch_lat=merch_lat,
        merch_long=merch_long,
        cust_lat=customer.lat,
        cust_long=customer.long,
        dob=customer.dob,
        is_fraud=label,
    )


def to_kafka_message(tx: Transaction, include_label: bool = False) -> str:
    """Serialise transaction to JSON string for Kafka."""
    d = asdict(tx)
    if not include_label:
        d.pop("is_fraud", None)
    return json.dumps(d)
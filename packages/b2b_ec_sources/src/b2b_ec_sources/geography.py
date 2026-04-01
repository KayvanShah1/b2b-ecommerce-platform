import ipaddress
import random
from functools import cache, lru_cache

from faker import Faker
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from b2b_ec_sources import get_iso_data


class GeographyParameters(BaseSettings):
    top_country_codes: list[str] = ["US", "CN", "DE", "JP", "IN", "GB", "FR", "IT", "BR", "CA"]
    top_country_weights: list[float] = [0.25, 0.15, 0.10, 0.08, 0.07, 0.05, 0.04, 0.03, 0.02, 0.01]
    top_country_share: float = Field(default=0.80, ge=0.0, le=1.0)
    country_weight_noise_sigma: float = Field(default=0.08, ge=0.0)
    min_weight_floor: float = Field(default=0.001, gt=0.0)

    trade_domestic_share: float = Field(default=0.50, ge=0.0, le=1.0)
    trade_regional_share: float = Field(default=0.30, ge=0.0, le=1.0)
    trade_global_share: float = Field(default=0.20, ge=0.0, le=1.0)
    trade_lane_noise_sigma: float = Field(default=0.07, ge=0.0)

    default_locale: str = "en_US"

    model_config = SettingsConfigDict(env_prefix="GEO_", extra="ignore")

    def model_post_init(self, __context):
        if len(self.top_country_codes) != len(self.top_country_weights):
            raise ValueError("top_country_codes and top_country_weights must have equal length")
        if not self.top_country_codes:
            raise ValueError("top_country_codes cannot be empty")
        if any(w < 0 for w in self.top_country_weights):
            raise ValueError("top_country_weights must be >= 0")
        lane_sum = self.trade_domestic_share + self.trade_regional_share + self.trade_global_share
        if lane_sum <= 0:
            raise ValueError("trade lane shares must sum to > 0")


GEO = GeographyParameters()


ISO_TO_FAKER_LOCALE = {
    "US": "en_US",
    "CN": "zh_CN",
    "DE": "de_DE",
    "JP": "ja_JP",
    "IN": "en_IN",
    "GB": "en_GB",
    "FR": "fr_FR",
    "IT": "it_IT",
    "BR": "pt_BR",
    "CA": "en_CA",
}


COUNTRY_REGION = {
    "US": "AMER",
    "CA": "AMER",
    "BR": "AMER",
    "MX": "AMER",
    "AR": "AMER",
    "GB": "EMEA",
    "DE": "EMEA",
    "FR": "EMEA",
    "IT": "EMEA",
    "ES": "EMEA",
    "NL": "EMEA",
    "AE": "EMEA",
    "QA": "EMEA",
    "SA": "EMEA",
    "CN": "APAC",
    "JP": "APAC",
    "IN": "APAC",
    "AU": "APAC",
    "SG": "APAC",
    "KR": "APAC",
}


COUNTRY_IP_CIDRS = {
    "US": ["3.0.0.0/8", "8.0.0.0/8", "13.0.0.0/8", "18.0.0.0/8", "20.0.0.0/8"],
    "CN": ["1.0.1.0/24", "1.2.0.0/15", "36.0.0.0/8", "42.0.0.0/8"],
    "DE": ["5.56.0.0/13", "46.5.0.0/16", "79.192.0.0/10", "91.0.0.0/10"],
    "JP": ["1.0.16.0/20", "27.96.0.0/11", "43.224.0.0/12", "106.72.0.0/13"],
    "IN": ["1.6.0.0/15", "14.96.0.0/11", "27.56.0.0/13", "49.32.0.0/11"],
    "GB": ["2.24.0.0/13", "5.80.0.0/13", "51.6.0.0/15", "82.0.0.0/11"],
    "FR": ["2.0.0.0/11", "5.48.0.0/12", "37.160.0.0/11", "90.0.0.0/10"],
    "IT": ["2.32.0.0/11", "5.88.0.0/13", "79.0.0.0/10", "93.32.0.0/11"],
    "BR": ["45.4.0.0/14", "177.0.0.0/9", "179.0.0.0/8", "186.192.0.0/10"],
    "CA": ["24.0.0.0/8", "47.54.0.0/16", "64.0.0.0/8", "99.224.0.0/11"],
}


def normalize_weights(weights: list[float]) -> list[float]:
    if not weights:
        return []
    total = sum(weights)
    if total <= 0:
        return [1.0 / len(weights)] * len(weights)
    return [w / total for w in weights]


def jittered_weights(weights: list[float], sigma: float, floor: float = 0.001) -> list[float]:
    base = normalize_weights(weights)
    if sigma <= 0:
        return base
    jittered: list[float] = []
    for w in base:
        factor = max(0.0, 1.0 + random.gauss(0.0, sigma))
        jittered.append(max(floor, w * factor))
    return normalize_weights(jittered)


@cache
def all_iso_codes() -> tuple[str, ...]:
    return tuple(item["code"] for item in get_iso_data())


def _rest_country_codes() -> list[str]:
    top = {c.upper() for c in GEO.top_country_codes}
    rest = [code for code in all_iso_codes() if code not in top]
    return rest if rest else list(top)


def build_country_distribution() -> dict[str, object]:
    iso_set = set(all_iso_codes())
    top_pairs = [
        (code.upper(), weight)
        for code, weight in zip(GEO.top_country_codes, GEO.top_country_weights, strict=False)
        if code.upper() in iso_set
    ]
    if not top_pairs:
        top_pairs = [("US", 1.0)] if "US" in iso_set else [(next(iter(iso_set)), 1.0)]

    top_codes = [pair[0] for pair in top_pairs]
    top_weights = jittered_weights([pair[1] for pair in top_pairs], sigma=GEO.country_weight_noise_sigma)
    top_share = GEO.top_country_share + random.gauss(0.0, GEO.country_weight_noise_sigma * 0.25)
    top_share = min(0.95, max(0.05, top_share))
    return {
        "top_codes": top_codes,
        "top_weights": top_weights,
        "top_share": top_share,
        "rest_codes": _rest_country_codes(),
    }


def sample_country_code(distribution: dict[str, object] | None = None) -> str:
    dist = distribution or build_country_distribution()
    top_codes: list[str] = list(dist["top_codes"])
    top_weights: list[float] = list(dist["top_weights"])
    top_share = float(dist["top_share"])
    rest_codes: list[str] = list(dist["rest_codes"])

    if top_codes and random.random() < top_share:
        return random.choices(top_codes, weights=top_weights, k=1)[0]
    if rest_codes:
        return random.choice(rest_codes)
    return random.choice(top_codes) if top_codes else "US"


def country_weight_for_sampling(country_code: str, distribution: dict[str, object] | None = None) -> float:
    dist = distribution or build_country_distribution()
    code = (country_code or "").upper()
    top_codes: list[str] = list(dist["top_codes"])
    top_weights: list[float] = list(dist["top_weights"])
    top_share = float(dist["top_share"])
    rest_codes: list[str] = list(dist["rest_codes"])

    if code in top_codes:
        idx = top_codes.index(code)
        return max(GEO.min_weight_floor, top_weights[idx] * top_share)
    rest_share = max(GEO.min_weight_floor, 1.0 - top_share)
    rest_count = max(1, len(rest_codes))
    return max(GEO.min_weight_floor, rest_share / rest_count)


def country_sampling_weights(
    country_codes: list[str], distribution: dict[str, object] | None = None
) -> list[float]:
    raw = [country_weight_for_sampling(code, distribution=distribution) for code in country_codes]
    return normalize_weights(raw)


def locale_for_country(country_code: str | None) -> str:
    if not country_code:
        return GEO.default_locale
    return ISO_TO_FAKER_LOCALE.get(country_code.upper(), GEO.default_locale)


@lru_cache(maxsize=128)
def _faker_for_locale(locale: str) -> Faker:
    return Faker(locale)


def localized_company_name(country_code: str | None) -> str:
    faker = _faker_for_locale(locale_for_country(country_code))
    return faker.company().replace("\n", " ").strip()


def localized_first_last_name(country_code: str | None) -> tuple[str, str]:
    faker = _faker_for_locale(locale_for_country(country_code))
    first = faker.first_name().replace("\n", " ").strip()
    last = faker.last_name().replace("\n", " ").strip()
    return first, last


def localized_full_name(country_code: str | None) -> str:
    faker = _faker_for_locale(locale_for_country(country_code))
    return faker.name().replace("\n", " ").strip()


def localized_phone(country_code: str | None) -> str:
    faker = _faker_for_locale(locale_for_country(country_code))
    return faker.phone_number().replace("\n", " ").strip()


def region_for_country(country_code: str | None) -> str:
    if not country_code:
        return "ROW"
    return COUNTRY_REGION.get(country_code.upper(), "ROW")


def classify_trade_lane(client_country_code: str | None, supplier_country_code: str | None) -> str:
    if client_country_code and supplier_country_code and client_country_code.upper() == supplier_country_code.upper():
        return "domestic"
    if region_for_country(client_country_code) == region_for_country(supplier_country_code):
        return "regional"
    return "global"


def sample_trade_lane() -> str:
    lane_weights = jittered_weights(
        [GEO.trade_domestic_share, GEO.trade_regional_share, GEO.trade_global_share],
        sigma=GEO.trade_lane_noise_sigma,
        floor=GEO.min_weight_floor,
    )
    return random.choices(["domestic", "regional", "global"], weights=lane_weights, k=1)[0]


def _sample_ipv4_from_network(cidr: str) -> str:
    network = ipaddress.ip_network(cidr)
    if network.num_addresses <= 2:
        return str(network.network_address)
    first = int(network.network_address) + 1
    last = int(network.broadcast_address) - 1
    for _ in range(16):
        value = random.randint(first, last)
        addr = ipaddress.IPv4Address(value)
        if addr.is_global:
            return str(addr)
    return sample_public_ipv4()


def sample_public_ipv4() -> str:
    while True:
        value = random.randint(1, (2**32) - 2)
        addr = ipaddress.IPv4Address(value)
        if addr.is_global:
            return str(addr)


def sample_ip_for_country(country_code: str | None) -> str:
    if not country_code:
        return sample_public_ipv4()
    cidrs = COUNTRY_IP_CIDRS.get(country_code.upper())
    if not cidrs:
        return sample_public_ipv4()
    return _sample_ipv4_from_network(random.choice(cidrs))

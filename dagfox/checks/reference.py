"""Reference data and validators: ISO 17442 (LEI) and ISO 3166-1 alpha-2."""

from __future__ import annotations

import re

LEI_PATTERN = re.compile(r"^[A-Z0-9]{18}[0-9]{2}$")
ELF_CODE_PATTERN = re.compile(r"^[A-Z0-9]{4}$")

# ISO 3166-1 alpha-2, plus XK (Kosovo), which GLEIF uses.
ISO_COUNTRIES = frozenset(
    """
    AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS
    BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE
    EG EH ER ES ET FI FJ FK FM FO FR GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM
    HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC
    LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA
    NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW
    SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO
    TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW XK
    """.split()
)


def _to_digits(s: str) -> int:
    return int("".join(str(int(ch, 36)) for ch in s))


def lei_check_digits(base18: str) -> str:
    """ISO 7064 MOD 97-10 check digits for an 18-character LEI prefix."""
    return f"{98 - _to_digits(base18 + '00') % 97:02d}"


def lei_is_valid(lei: str | None) -> bool:
    return bool(lei) and bool(LEI_PATTERN.match(lei)) and _to_digits(lei) % 97 == 1


def country_is_valid(code: str | None) -> bool:
    return code in ISO_COUNTRIES


def normalize_country(code: str | None) -> str | None:
    """Deterministic repair for formatting faults ('us ', 'de'). Returns None if not repairable."""
    if code is None:
        return None
    candidate = code.strip().upper()
    return candidate if candidate in ISO_COUNTRIES else None

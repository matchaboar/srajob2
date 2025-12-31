from __future__ import annotations

import re

# Primitive tokens
DIGIT_PATTERN = r"\d"
MIN_THREE_DIGIT_PATTERN = r"\d{3,}"
WHITESPACE_PATTERN = r"\s+"
MULTI_SPACE_PATTERN = r"\s+"
NON_ALNUM_PATTERN = r"[^a-z0-9]+"
NON_ALNUM_SPACE_PATTERN = r"[^a-z0-9 ]+"
NUMBER_TOKEN_PATTERN = r"[0-9][0-9,\.]+"
SALARY_NUMBER_PATTERN = r"(?:\d{2,3}(?:[.,]\d{3})+|\d{4,6})(?:\.\d{2})?"
HOURLY_NUMBER_PATTERN = r"\d{1,3}(?:\.\d{1,2})?"
PARENTHETICAL_PATTERN = r"\(.*?\)"

# Code fences and JSON cleanup
CODE_FENCE_START_PATTERN = r"^```[a-zA-Z0-9_-]*\n?"
CODE_FENCE_END_PATTERN = r"\n?```$"
CODE_FENCE_CONTENT_PATTERN = r"```(?:json)?\n(?P<content>.*?)\n```"
CODE_FENCE_JSON_OBJECT_PATTERN = r"```(?:json)?\s*(\{.*?\})\s*```"
INVALID_JSON_ESCAPE_PATTERN = r"\\(?![\"\\/bfnrtu])"

# URL / link patterns
URL_PATTERN = r"https?://[^\s\"'<>]+"
MARKDOWN_LINK_PATTERN = r"(?<!!)\[([^\]]+)\]\(([^)\s]+)\)"
GREENHOUSE_URL_PATTERN = r"https?://[\w.-]*greenhouse\.io/[^\s\"'<>]+"
GREENHOUSE_BOARDS_PATH_PATTERN = r"/boards/([^/]+)/jobs"
ASHBY_JOB_URL_PATTERN = r"https?://jobs\.ashbyhq\.com/[^\s\"'>\)\]\*#]+"
ASHBY_JOB_SLUG_PATTERN = r"https?://jobs\.ashbyhq\.com/([^/\"'\s]+)"
CONFLUENT_JOB_PATH_PATTERN = r"/jobs/job/[0-9a-f-]{8,}"

# Company / US signals
COMPANY_NORMALIZE_PATTERN = r"[^a-z0-9]+"
COMPANY_NORMALIZE_RE = re.compile(COMPANY_NORMALIZE_PATTERN)
ZIP_CODE_PATTERN = r"\b\d{5}(?:-\d{4})?\b"
ZIP_CODE_RE = re.compile(ZIP_CODE_PATTERN)
US_ABBREVIATION_PATTERN = r"\bU\.?S\.?A?\b"
US_STATE_CODE_PATTERN_TEMPLATE = r"\b{code}\b"

# Job title / location / compensation patterns
TITLE_PATTERN = r"^[ \t]*#{1,6}\s+(?P<title>.+)$"
TITLE_LOCATION_PAREN_PATTERN = r"(.+?)[\[(]\s*(.+?)\s*[\)\]]$"
TITLE_BAR_PATTERN = r"^(?P<title>.+?)\s+\|\s+.+$"
TITLE_IN_BAR_PATTERN = r"^(?P<title>.+?)\s+in\s+(?P<location>.+?)\s+\|\s+.+$"
TITLE_IN_BAR_COMPANY_PATTERN = r"^(?P<title>.+?)\s+in\s+(?P<location>.+?)\s+\|\s+(?P<company>.+)$"
LEVEL_PATTERN = (
    r"\b(?P<level>intern|junior|mid(?:-level)?|mid|sr|senior|staff|principal|lead|manager|director|vp|"
    r"cto|chief technology officer)\b"
)
LOCATION_PATTERN = r"\b(?:location|office|based\s+in)\b\s*(?:[:\-–]\s*|\s+)(?P<location>[^\n,;]+(?:,\s*[^\n,;]+)?)"
SIMPLE_LOCATION_LINE_PATTERN = r"^[ \t]*(?P<location>[A-Z][\w .'-]+,\s*[A-Z][\w .'-]+)\s*$"
WORK_FROM_PATTERN = r"\bwork(?:ing)?\s+from\s+(?P<location>.+)$"
REMOTE_PATTERN = r"\b(remote(-first)?|hybrid|onsite|on-site)\b"
LOCATION_ANYWHERE_PATTERN = r"[A-Za-z].*,\s*[A-Za-z]"
LOCATION_FULL_PATTERN = r"(?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z][A-Za-z .'-]{3,})"
LOCATION_LABEL_PATTERN = r"location[:\-\s]+(?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z]{2})"
LOCATION_CITY_STATE_PATTERN = r"(?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z]{2})"
LOCATION_PAREN_PATTERN = r"\((?P<location>[A-Z][A-Za-z .'-]+,\s*[A-Z]{2})\)"
LOCATION_LINE_PATTERN = r"^\s*location\b\s*[:\-–]?\s*(?P<location>.+)$"
LOCATION_KEY_BOUNDARY_PATTERN_TEMPLATE = r"(?:^|\s){key}(?:\s|$)"
LOCATION_SPLIT_PATTERN = r"[;|/]"
LOCATION_TOKEN_SPLIT_PATTERN = r"[,\s]+"
LOCATION_PREFIX_PATTERN = r"^(?:location|office|offices|based in)\s*[:\-–]\s*"
REQUEST_ID_PATTERN = r"\[Request ID:\s*([^\]]+)\]"
COUNTRY_CODE_PATTERN = r"^[A-Z]{2}$"
ERROR_404_PATTERN = r"\b404\b"

SALARY_PATTERN = (
    r"\$\s*(?P<low>" + SALARY_NUMBER_PATTERN + r")"
    r"(?:\s*(?:[-–—]|&ndash;|&mdash;)\s*(?:(?:USD|EUR|GBP)\s*)?\$?\s*"
    r"(?P<high>" + SALARY_NUMBER_PATTERN + r"))?"
    r"\s*(?P<period>per\s+year|per\s+annum|annual|yr|year|/year|per\s+hour|hr|hour)?"
)
SALARY_HOURLY_RANGE_PATTERN = (
    r"(?:(?:USD|US\$)\s*)?\$\s*(?P<low>" + HOURLY_NUMBER_PATTERN + r")"
    r"\s*(?:[-–—]|to)\s*"
    r"(?:(?:USD|US\$)\s*)?\$?\s*(?P<high>" + HOURLY_NUMBER_PATTERN + r")"
    r"\s*(?:/\s*hr|/\s*hour|per\s*hour|hourly|hr\b)"
)
SALARY_RANGE_LABEL_PATTERN = (
    r"(?:salary|compensation|pay)\s+range\s*[:=\-–—]\s*"
    r"(?P<low>" + SALARY_NUMBER_PATTERN + r")"
    r"(?:\s*(?:[-–—]|&ndash;|&mdash;)\s*(?P<high>" + SALARY_NUMBER_PATTERN + r"))?"
    r"(?:\s*(?P<code>USD|EUR|GBP))?"
)
SALARY_BETWEEN_PATTERN = (
    r"(?:between|from)\s+"
    r"(?:(?:USD|EUR|GBP)\s*)?\$?\s*(?P<low>" + SALARY_NUMBER_PATTERN + r")"
    r"(?:\s*[^\d$]{0,80}?)?\s*(?:and|to)\s*"
    r"(?:(?:USD|EUR|GBP)\s*)?\$?\s*(?P<high>" + SALARY_NUMBER_PATTERN + r")"
)
SALARY_K_PATTERN = (
    r"(?P<currency>[$£€])?\s*(?P<low>\d{2,3})\s*[kK]"
    r"\s*(?:([-–—]|&ndash;|&mdash;)\s*(?P<high>\d{2,3})\s*[kK])?"
    r"\s*(?P<code>USD|EUR|GBP)?"
)
COMP_USD_RANGE_PATTERN = (
    r"\$\s*(?P<low>\d{2,3}(?:[.,]\d{3})?)(?:\s*[-–]\s*\$?\s*(?P<high>\d{2,3}(?:[.,]\d{3})?))?"
)
COMP_INR_RANGE_PATTERN = (
    r"[₹]\s*(?P<low>\d{1,3}(?:[.,]\d{3})?)(?:\s*[-–]\s*[₹]?\s*(?P<high>\d{1,3}(?:[.,]\d{3})?))?"
)
COMP_K_PATTERN = r"(?P<value>\d{2,3})k"
COMP_LPA_PATTERN = r"(?P<value>\d{1,3})\s*(lpa|lakh)"
RETIREMENT_PLAN_PATTERN = r"\b401\s*\(?k\)?\b"
NON_NUMERIC_DOT_PATTERN = r"[^0-9.]"
NON_NUMERIC_PATTERN = r"[^0-9]"
APPLY_WORD_PATTERN = r"\bapply\b"

INR_CURRENCY_PATTERNS = [
    r"₹",
    r"\brupees?\b",
    r"\brupee\b",
    r"\bINR\b",
    r"\blakh\b",
    r"\blpa\b",
]
GBP_CURRENCY_PATTERNS = [r"£", r"\bGBP\b"]
EUR_CURRENCY_PATTERNS = [r"€", r"\bEUR\b"]
AUD_CURRENCY_PATTERNS = [r"\bAUD\b", r"\bA\\$"]
CAD_CURRENCY_PATTERNS = [r"\bCAD\b", r"\bC\\$"]

# HTML extraction / cleanup
HTML_TAG_PATTERN = r"<[^>]+>"
PRE_CONTENT_PATTERN = r"<pre[^>]*>(?P<content>.*?)</pre>"
PRE_PATTERN = re.compile(PRE_CONTENT_PATTERN, flags=re.IGNORECASE | re.DOTALL)
JSON_OBJECT_PATTERN = r"{.*}"
JSON_ARRAY_PATTERN = r"\[.*\]"
HTML_LINE_BREAK_PATTERN = r"<br\\s*/?>"
HTML_PARAGRAPH_CLOSE_PATTERN = r"</p\\s*>"
HTML_PARAGRAPH_OPEN_PATTERN = r"<p[^>]*>"
HTML_LIST_ITEM_OPEN_PATTERN = r"<li[^>]*>"
HTML_SCRIPT_OR_STYLE_BLOCK_PATTERN = r"<(script|style)[^>]*>.*?</\\1>"
HORIZONTAL_WHITESPACE_PATTERN = r"[ \t]+"
LINE_WRAPPED_WHITESPACE_PATTERN = r"\\s*\\n\\s*"
MULTI_NEWLINE_PATTERN = r"\\n{3,}"

# Navigation / cookie UI
_NAV_MENU_SEQUENCE = [
    "Welcome",
    "Culture",
    "Workplace Benefits",
    "Candidate Experience",
    "Diversity, Equity & Inclusion",
    "Learning & Development",
    "Pup Culture Blog",
    "Teams",
    "Engineering",
    "General & Administrative",
    "Marketing",
    "Product Design",
    "Product Management",
    "Sales",
    "Technical Solutions",
    "Early Career & Internships",
    "Locations",
    "Americas",
    "Asia Pacific",
    "EMEA",
    "Remote",
    "All Jobs",
]
NAV_BLOCK_PATTERN = (
    r"(?:"
    + r"\s+".join(re.escape(term) for term in _NAV_MENU_SEQUENCE)
    + r")(?:\s+###\s*Careers)?(?:\s+"
    + r"\s+".join(re.escape(term) for term in _NAV_MENU_SEQUENCE)
    + r")?"
)
_COOKIE_SIGNAL_PATTERN = (
    r"(cookie\s+preferences|cookie\s+policy|cookie\s+consent|cookie\s+settings|"
    r"your\s+choice\s+regarding\s+cookies|this\s+website\s+uses\s+cookies|"
    r"accept\s+all|reject\s+all|save\s+and\s+close|manage\s+cookies|"
    r"essential\s+cookies|performance\s+cookies|functional\s+cookies|advertising\s+cookies|"
    r"cookiebot|onetrust|trustarc|optimizely|google\s+analytics|microsoft\s+clarity|"
    r"tag\s+manager|gtm)"
)

# Compiled helpers
_COOKIE_SIGNAL_RE = re.compile(_COOKIE_SIGNAL_PATTERN, flags=re.IGNORECASE)
_COOKIE_WORD_RE = re.compile(r"\bcookies?\b", flags=re.IGNORECASE)
_COOKIE_UI_CONTROL_RE = re.compile(
    r"^(accept\s+all|reject\s+all|save\s+and\s+close|cookie\s+preferences|preferences)$",
    flags=re.IGNORECASE,
)
_HTML_TAG_RE = re.compile(HTML_TAG_PATTERN)
_LISTING_SELECT_RE = re.compile(
    r"\bselect\s+(department|country|location|city|state|category|team)\b",
    flags=re.IGNORECASE,
)
_LISTING_TABLE_HEADER_RE = re.compile(
    r"\|\s*\*\*\s*role\s*\*\*.*\|\s*\*\*\s*(team|department)\s*\*\*.*\|\s*\*\*\s*type\s*\*\*.*\|\s*\*\*\s*location\s*\*\*",
    flags=re.IGNORECASE,
)
_NAV_BLOCK_REGEX = re.compile(NAV_BLOCK_PATTERN, flags=re.IGNORECASE)
_TITLE_RE = re.compile(TITLE_PATTERN, flags=re.IGNORECASE | re.MULTILINE)
_TITLE_BAR_RE = re.compile(TITLE_BAR_PATTERN, flags=re.IGNORECASE)
_TITLE_IN_BAR_RE = re.compile(TITLE_IN_BAR_PATTERN, flags=re.IGNORECASE)
_TITLE_IN_BAR_COMPANY_RE = re.compile(TITLE_IN_BAR_COMPANY_PATTERN, flags=re.IGNORECASE)
_TITLE_LOCATION_PAREN_RE = re.compile(TITLE_LOCATION_PAREN_PATTERN, flags=re.IGNORECASE)
_LEVEL_RE = re.compile(LEVEL_PATTERN, flags=re.IGNORECASE)
_LOCATION_RE = re.compile(LOCATION_PATTERN, flags=re.IGNORECASE)
_SIMPLE_LOCATION_LINE_RE = re.compile(SIMPLE_LOCATION_LINE_PATTERN, flags=re.MULTILINE)
_WORK_FROM_RE = re.compile(WORK_FROM_PATTERN, flags=re.IGNORECASE)
_SALARY_RE = re.compile(SALARY_PATTERN, flags=re.IGNORECASE)
_SALARY_HOURLY_RANGE_RE = re.compile(SALARY_HOURLY_RANGE_PATTERN, flags=re.IGNORECASE)
_SALARY_RANGE_LABEL_RE = re.compile(SALARY_RANGE_LABEL_PATTERN, flags=re.IGNORECASE)
_SALARY_BETWEEN_RE = re.compile(SALARY_BETWEEN_PATTERN, flags=re.IGNORECASE)
_SALARY_K_RE = re.compile(SALARY_K_PATTERN, flags=re.IGNORECASE)
_REMOTE_RE = re.compile(REMOTE_PATTERN, flags=re.IGNORECASE)

# Base URL meta tags
BASE_HREF_PATTERN = r"<base[^>]+href=\"(?P<url>[^\"]+)\""
OG_URL_PATTERN = r"property=\"og:url\"[^>]+content=\"(?P<url>[^\"]+)\""
CANONICAL_URL_PATTERN = r"rel=\"canonical\"[^>]+href=\"(?P<url>[^\"]+)\""
NAME_OG_URL_PATTERN = r"name=\"og:url\"[^>]+content=\"(?P<url>[^\"]+)\""
BASE_URL_META_PATTERNS = (BASE_HREF_PATTERN, OG_URL_PATTERN, CANONICAL_URL_PATTERN)
NETFLIX_LISTING_URL_PATTERNS = (CANONICAL_URL_PATTERN, OG_URL_PATTERN, NAME_OG_URL_PATTERN)

# Site handlers: Avature
AVATURE_JOB_DETAIL_PATH_PATTERN = r"/careers/JobDetail/[^\"'\s>]+"
AVATURE_JOB_DETAIL_PATH_RE = re.compile(AVATURE_JOB_DETAIL_PATH_PATTERN, re.IGNORECASE)
AVATURE_JOB_DETAIL_URL_PATTERN = r"https?://[^\"'\s>]+/careers/JobDetail/[^\"'\s>]+"
AVATURE_JOB_DETAIL_URL_RE = re.compile(AVATURE_JOB_DETAIL_URL_PATTERN, re.IGNORECASE)
AVATURE_PAGINATION_PATH_PATTERN = (
    r"/careers/(?:SearchJobs|SearchJobsData)(?:/[^\"'\s>]*)?[?&][^\"'\s>]*jobOffset=\d+"
)
AVATURE_PAGINATION_PATH_RE = re.compile(AVATURE_PAGINATION_PATH_PATTERN, re.IGNORECASE)
AVATURE_PAGINATION_URL_PATTERN = (
    r"https?://[^\"'\s>]+/careers/(?:SearchJobs|SearchJobsData)"
    r"(?:/[^\"'\s>]*)?[?&][^\"'\s>]*jobOffset=\d+"
)
AVATURE_PAGINATION_URL_RE = re.compile(AVATURE_PAGINATION_URL_PATTERN, re.IGNORECASE)
AVATURE_BASE_URL_PATTERN = r"https?://[^\"'\s>]+/careers/[^\"'\s>]*"
AVATURE_BASE_URL_RE = re.compile(AVATURE_BASE_URL_PATTERN, re.IGNORECASE)
AVATURE_PAGE_RANGE_PATTERN = r"(?P<start>\d+)\s*-\s*(?P<end>\d+)\s*of\s*(?P<total>\d+)"
AVATURE_PAGE_RANGE_RE = re.compile(AVATURE_PAGE_RANGE_PATTERN, re.IGNORECASE)
AVATURE_JOB_RECORDS_PER_PAGE_PATTERN = r"jobRecordsPerPage\"?\s*[:=]\s*\"?(?P<count>\d+)\"?"
AVATURE_JOB_RECORDS_PER_PAGE_RE = re.compile(AVATURE_JOB_RECORDS_PER_PAGE_PATTERN, re.IGNORECASE)
AVATURE_RESULTS_ARIA_PATTERN = r"aria-label=\"\s*(?P<count>\d+)\s+results"
AVATURE_RESULTS_ARIA_RE = re.compile(AVATURE_RESULTS_ARIA_PATTERN, re.IGNORECASE)

# Site handlers: Workday
WORKDAY_JOB_DETAIL_URL_PATTERN = r"https?://[^\"'\\s>]+/job/[^\"'\\s>]+"
WORKDAY_JOB_DETAIL_URL_RE = re.compile(WORKDAY_JOB_DETAIL_URL_PATTERN, re.IGNORECASE)
WORKDAY_JOB_DETAIL_PATH_PATTERN = r"/job/[^\"'\\s>]+"
WORKDAY_JOB_DETAIL_PATH_RE = re.compile(WORKDAY_JOB_DETAIL_PATH_PATTERN, re.IGNORECASE)
WORKDAY_JOB_TITLE_ANCHOR_PATTERN = (
    r"<a[^>]+data-automation-id=[\"']jobTitle[\"'][^>]+href=[\"'](?P<href>[^\"']+)[\"']"
)
WORKDAY_JOB_TITLE_ANCHOR_RE = re.compile(WORKDAY_JOB_TITLE_ANCHOR_PATTERN, re.IGNORECASE)
WORKDAY_PAGE_RANGE_PATTERN = r"(?P<start>\\d+)\\s*-\\s*(?P<end>\\d+)\\s*of\\s*(?P<total>\\d+)\\s+jobs"
WORKDAY_PAGE_RANGE_RE = re.compile(WORKDAY_PAGE_RANGE_PATTERN, re.IGNORECASE)
WORKDAY_BASE_URL_PATTERN = r"https?://[^\"'\\s>]*myworkdayjobs\\.com/[^\"'\\s>]*"
WORKDAY_BASE_URL_RE = re.compile(WORKDAY_BASE_URL_PATTERN, re.IGNORECASE)

# Site handlers: Netflix
SMART_APPLY_PATTERN = re.compile(
    r"<code[^>]*id=\"smartApplyData\"[^>]*>(?P<content>.*?)</code>",
    flags=re.IGNORECASE | re.DOTALL,
)

# Site handlers: Greenhouse
JOB_ID_PATH_PATTERN = r"/jobs/(\d+)"

# SpiderCloud helpers
HTML_BR_TAG_PATTERN = r"(?i)<br\\s*/?>"
SPIDERCLOUD_HTML_PARAGRAPH_CLOSE_PATTERN = r"(?i)</p>"
HTML_SCRIPT_BLOCK_PATTERN = r"(?is)<script[^>]*>.*?</script>"
HTML_STYLE_BLOCK_PATTERN = r"(?is)<style[^>]*>.*?</style>"
SPIDERCLOUD_MULTI_NEWLINE_PATTERN = r"\n{3,}"
MARKDOWN_HEADING_PATTERN = r"^#{1,6}\s*(.+)$"
MARKDOWN_HEADING_PREFIX_PATTERN = r"^#{1,6}\s*"
SLUG_SEPARATOR_PATTERN = r"[-_]+"
QUERY_STRING_PATTERN = r"\?.*$"
CAPTCHA_WORD_PATTERN = r"\bcaptcha\b"
CAPTCHA_PROVIDER_PATTERN = r"(?:g-recaptcha|recaptcha/api2|i[' ]?m not a robot|verify you are human)"
JSON_LD_SCRIPT_PATTERN = (
    r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(?P<payload>.*?)</script>"
)

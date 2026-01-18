"""Lightweight runtime shim for crawl4ai when the real package isn't installed."""

from __future__ import annotations

from typing import Any
from inspect import signature as _mutmut_signature
from typing import Annotated
from typing import Callable
from typing import ClassVar


MutantDict = Annotated[dict[str, Callable], "Mutant"]


def _mutmut_trampoline(orig, mutants, call_args, call_kwargs, self_arg = None):
    """Forward call to original or mutated function, depending on the environment"""
    import os
    mutant_under_test = os.environ['MUTANT_UNDER_TEST']
    if mutant_under_test == 'fail':
        from mutmut.__main__ import MutmutProgrammaticFailException
        raise MutmutProgrammaticFailException('Failed programmatically')      
    elif mutant_under_test == 'stats':
        from mutmut.__main__ import record_trampoline_hit
        record_trampoline_hit(orig.__module__ + '.' + orig.__name__)
        result = orig(*call_args, **call_kwargs)
        return result
    prefix = orig.__module__ + '.' + orig.__name__ + '__mutmut_'
    if not mutant_under_test.startswith(prefix):
        result = orig(*call_args, **call_kwargs)
        return result
    mutant_name = mutant_under_test.rpartition('.')[-1]
    if self_arg is not None:
        # call to a class method where self is not bound
        result = mutants[mutant_name](self_arg, *call_args, **call_kwargs)
    else:
        result = mutants[mutant_name](*call_args, **call_kwargs)
    return result


class BrowserConfig:
    def xǁBrowserConfigǁ__init____mutmut_orig(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_1(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = None
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_2(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get(None)
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_3(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("XXheadlessXX")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_4(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("HEADLESS")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_5(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = None
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_6(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get(None)
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_7(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("XXbrowser_typeXX")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_8(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("BROWSER_TYPE")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_9(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = None
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_10(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get(None)
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_11(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("XXverboseXX")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_12(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("VERBOSE")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_13(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = None
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_14(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get(None)
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_15(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("XXbrowser_modeXX")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_16(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("BROWSER_MODE")
        self.user_agent_mode: str | None = kwargs.get("user_agent_mode")
    def xǁBrowserConfigǁ__init____mutmut_17(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = None
    def xǁBrowserConfigǁ__init____mutmut_18(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get(None)
    def xǁBrowserConfigǁ__init____mutmut_19(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("XXuser_agent_modeXX")
    def xǁBrowserConfigǁ__init____mutmut_20(self, *args: Any, **kwargs: Any) -> None:
        self.headless: bool | None = kwargs.get("headless")
        self.browser_type: str | None = kwargs.get("browser_type")
        self.verbose: bool | None = kwargs.get("verbose")
        self.browser_mode: str | None = kwargs.get("browser_mode")
        self.user_agent_mode: str | None = kwargs.get("USER_AGENT_MODE")
    
    xǁBrowserConfigǁ__init____mutmut_mutants : ClassVar[MutantDict] = {
    'xǁBrowserConfigǁ__init____mutmut_1': xǁBrowserConfigǁ__init____mutmut_1, 
        'xǁBrowserConfigǁ__init____mutmut_2': xǁBrowserConfigǁ__init____mutmut_2, 
        'xǁBrowserConfigǁ__init____mutmut_3': xǁBrowserConfigǁ__init____mutmut_3, 
        'xǁBrowserConfigǁ__init____mutmut_4': xǁBrowserConfigǁ__init____mutmut_4, 
        'xǁBrowserConfigǁ__init____mutmut_5': xǁBrowserConfigǁ__init____mutmut_5, 
        'xǁBrowserConfigǁ__init____mutmut_6': xǁBrowserConfigǁ__init____mutmut_6, 
        'xǁBrowserConfigǁ__init____mutmut_7': xǁBrowserConfigǁ__init____mutmut_7, 
        'xǁBrowserConfigǁ__init____mutmut_8': xǁBrowserConfigǁ__init____mutmut_8, 
        'xǁBrowserConfigǁ__init____mutmut_9': xǁBrowserConfigǁ__init____mutmut_9, 
        'xǁBrowserConfigǁ__init____mutmut_10': xǁBrowserConfigǁ__init____mutmut_10, 
        'xǁBrowserConfigǁ__init____mutmut_11': xǁBrowserConfigǁ__init____mutmut_11, 
        'xǁBrowserConfigǁ__init____mutmut_12': xǁBrowserConfigǁ__init____mutmut_12, 
        'xǁBrowserConfigǁ__init____mutmut_13': xǁBrowserConfigǁ__init____mutmut_13, 
        'xǁBrowserConfigǁ__init____mutmut_14': xǁBrowserConfigǁ__init____mutmut_14, 
        'xǁBrowserConfigǁ__init____mutmut_15': xǁBrowserConfigǁ__init____mutmut_15, 
        'xǁBrowserConfigǁ__init____mutmut_16': xǁBrowserConfigǁ__init____mutmut_16, 
        'xǁBrowserConfigǁ__init____mutmut_17': xǁBrowserConfigǁ__init____mutmut_17, 
        'xǁBrowserConfigǁ__init____mutmut_18': xǁBrowserConfigǁ__init____mutmut_18, 
        'xǁBrowserConfigǁ__init____mutmut_19': xǁBrowserConfigǁ__init____mutmut_19, 
        'xǁBrowserConfigǁ__init____mutmut_20': xǁBrowserConfigǁ__init____mutmut_20
    }
    
    def __init__(self, *args, **kwargs):
        result = _mutmut_trampoline(object.__getattribute__(self, "xǁBrowserConfigǁ__init____mutmut_orig"), object.__getattribute__(self, "xǁBrowserConfigǁ__init____mutmut_mutants"), args, kwargs, self)
        return result 
    
    __init__.__signature__ = _mutmut_signature(xǁBrowserConfigǁ__init____mutmut_orig)
    xǁBrowserConfigǁ__init____mutmut_orig.__name__ = 'xǁBrowserConfigǁ__init__'


class CacheMode:
    BYPASS = "BYPASS"


class CrawlerRunConfig:
    def xǁCrawlerRunConfigǁ__init____mutmut_orig(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_1(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = None
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_2(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get(None)
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_3(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("XXcache_modeXX")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_4(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("CACHE_MODE")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_5(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = None
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_6(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get(None)
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_7(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("XXexclude_external_linksXX")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_8(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("EXCLUDE_EXTERNAL_LINKS")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_9(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = None
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_10(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get(None)
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_11(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("XXword_count_thresholdXX")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_12(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("WORD_COUNT_THRESHOLD")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_13(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = None
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_14(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get(None)
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_15(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("XXextraction_strategyXX")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_16(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("EXTRACTION_STRATEGY")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_17(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = None
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_18(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get(None)
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_19(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("XXscan_full_pageXX")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_20(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("SCAN_FULL_PAGE")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_21(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = None
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_22(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get(None)
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_23(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("XXremove_overlay_elementsXX")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_24(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("REMOVE_OVERLAY_ELEMENTS")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_25(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = None
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_26(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get(None)
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_27(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("XXmagicXX")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_28(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("MAGIC")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_29(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = None
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_30(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get(None)
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_31(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("XXsimulate_userXX")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_32(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("SIMULATE_USER")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_33(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = None
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_34(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get(None)
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_35(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("XXsession_idXX")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_36(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("SESSION_ID")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_37(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = None
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_38(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get(None)
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_39(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("XXwait_forXX")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_40(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("WAIT_FOR")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_41(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = None
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_42(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get(None)
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_43(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("XXjs_codeXX")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_44(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("JS_CODE")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_45(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = None
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_46(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get(None)
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_47(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("XXjs_onlyXX")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_48(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("JS_ONLY")
        self.delay_before_return_html: int | None = kwargs.get("delay_before_return_html")
    def xǁCrawlerRunConfigǁ__init____mutmut_49(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = None
    def xǁCrawlerRunConfigǁ__init____mutmut_50(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get(None)
    def xǁCrawlerRunConfigǁ__init____mutmut_51(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("XXdelay_before_return_htmlXX")
    def xǁCrawlerRunConfigǁ__init____mutmut_52(self, *args: Any, **kwargs: Any) -> None:
        self.cache_mode = kwargs.get("cache_mode")
        self.exclude_external_links: bool | None = kwargs.get("exclude_external_links")
        self.word_count_threshold: int | None = kwargs.get("word_count_threshold")
        self.extraction_strategy = kwargs.get("extraction_strategy")
        self.scan_full_page: bool | None = kwargs.get("scan_full_page")
        self.remove_overlay_elements: bool | None = kwargs.get("remove_overlay_elements")
        self.magic: bool | None = kwargs.get("magic")
        self.simulate_user: bool | None = kwargs.get("simulate_user")
        self.session_id: str | None = kwargs.get("session_id")
        self.wait_for: str | None = kwargs.get("wait_for")
        self.js_code: str | None = kwargs.get("js_code")
        self.js_only: bool | None = kwargs.get("js_only")
        self.delay_before_return_html: int | None = kwargs.get("DELAY_BEFORE_RETURN_HTML")
    
    xǁCrawlerRunConfigǁ__init____mutmut_mutants : ClassVar[MutantDict] = {
    'xǁCrawlerRunConfigǁ__init____mutmut_1': xǁCrawlerRunConfigǁ__init____mutmut_1, 
        'xǁCrawlerRunConfigǁ__init____mutmut_2': xǁCrawlerRunConfigǁ__init____mutmut_2, 
        'xǁCrawlerRunConfigǁ__init____mutmut_3': xǁCrawlerRunConfigǁ__init____mutmut_3, 
        'xǁCrawlerRunConfigǁ__init____mutmut_4': xǁCrawlerRunConfigǁ__init____mutmut_4, 
        'xǁCrawlerRunConfigǁ__init____mutmut_5': xǁCrawlerRunConfigǁ__init____mutmut_5, 
        'xǁCrawlerRunConfigǁ__init____mutmut_6': xǁCrawlerRunConfigǁ__init____mutmut_6, 
        'xǁCrawlerRunConfigǁ__init____mutmut_7': xǁCrawlerRunConfigǁ__init____mutmut_7, 
        'xǁCrawlerRunConfigǁ__init____mutmut_8': xǁCrawlerRunConfigǁ__init____mutmut_8, 
        'xǁCrawlerRunConfigǁ__init____mutmut_9': xǁCrawlerRunConfigǁ__init____mutmut_9, 
        'xǁCrawlerRunConfigǁ__init____mutmut_10': xǁCrawlerRunConfigǁ__init____mutmut_10, 
        'xǁCrawlerRunConfigǁ__init____mutmut_11': xǁCrawlerRunConfigǁ__init____mutmut_11, 
        'xǁCrawlerRunConfigǁ__init____mutmut_12': xǁCrawlerRunConfigǁ__init____mutmut_12, 
        'xǁCrawlerRunConfigǁ__init____mutmut_13': xǁCrawlerRunConfigǁ__init____mutmut_13, 
        'xǁCrawlerRunConfigǁ__init____mutmut_14': xǁCrawlerRunConfigǁ__init____mutmut_14, 
        'xǁCrawlerRunConfigǁ__init____mutmut_15': xǁCrawlerRunConfigǁ__init____mutmut_15, 
        'xǁCrawlerRunConfigǁ__init____mutmut_16': xǁCrawlerRunConfigǁ__init____mutmut_16, 
        'xǁCrawlerRunConfigǁ__init____mutmut_17': xǁCrawlerRunConfigǁ__init____mutmut_17, 
        'xǁCrawlerRunConfigǁ__init____mutmut_18': xǁCrawlerRunConfigǁ__init____mutmut_18, 
        'xǁCrawlerRunConfigǁ__init____mutmut_19': xǁCrawlerRunConfigǁ__init____mutmut_19, 
        'xǁCrawlerRunConfigǁ__init____mutmut_20': xǁCrawlerRunConfigǁ__init____mutmut_20, 
        'xǁCrawlerRunConfigǁ__init____mutmut_21': xǁCrawlerRunConfigǁ__init____mutmut_21, 
        'xǁCrawlerRunConfigǁ__init____mutmut_22': xǁCrawlerRunConfigǁ__init____mutmut_22, 
        'xǁCrawlerRunConfigǁ__init____mutmut_23': xǁCrawlerRunConfigǁ__init____mutmut_23, 
        'xǁCrawlerRunConfigǁ__init____mutmut_24': xǁCrawlerRunConfigǁ__init____mutmut_24, 
        'xǁCrawlerRunConfigǁ__init____mutmut_25': xǁCrawlerRunConfigǁ__init____mutmut_25, 
        'xǁCrawlerRunConfigǁ__init____mutmut_26': xǁCrawlerRunConfigǁ__init____mutmut_26, 
        'xǁCrawlerRunConfigǁ__init____mutmut_27': xǁCrawlerRunConfigǁ__init____mutmut_27, 
        'xǁCrawlerRunConfigǁ__init____mutmut_28': xǁCrawlerRunConfigǁ__init____mutmut_28, 
        'xǁCrawlerRunConfigǁ__init____mutmut_29': xǁCrawlerRunConfigǁ__init____mutmut_29, 
        'xǁCrawlerRunConfigǁ__init____mutmut_30': xǁCrawlerRunConfigǁ__init____mutmut_30, 
        'xǁCrawlerRunConfigǁ__init____mutmut_31': xǁCrawlerRunConfigǁ__init____mutmut_31, 
        'xǁCrawlerRunConfigǁ__init____mutmut_32': xǁCrawlerRunConfigǁ__init____mutmut_32, 
        'xǁCrawlerRunConfigǁ__init____mutmut_33': xǁCrawlerRunConfigǁ__init____mutmut_33, 
        'xǁCrawlerRunConfigǁ__init____mutmut_34': xǁCrawlerRunConfigǁ__init____mutmut_34, 
        'xǁCrawlerRunConfigǁ__init____mutmut_35': xǁCrawlerRunConfigǁ__init____mutmut_35, 
        'xǁCrawlerRunConfigǁ__init____mutmut_36': xǁCrawlerRunConfigǁ__init____mutmut_36, 
        'xǁCrawlerRunConfigǁ__init____mutmut_37': xǁCrawlerRunConfigǁ__init____mutmut_37, 
        'xǁCrawlerRunConfigǁ__init____mutmut_38': xǁCrawlerRunConfigǁ__init____mutmut_38, 
        'xǁCrawlerRunConfigǁ__init____mutmut_39': xǁCrawlerRunConfigǁ__init____mutmut_39, 
        'xǁCrawlerRunConfigǁ__init____mutmut_40': xǁCrawlerRunConfigǁ__init____mutmut_40, 
        'xǁCrawlerRunConfigǁ__init____mutmut_41': xǁCrawlerRunConfigǁ__init____mutmut_41, 
        'xǁCrawlerRunConfigǁ__init____mutmut_42': xǁCrawlerRunConfigǁ__init____mutmut_42, 
        'xǁCrawlerRunConfigǁ__init____mutmut_43': xǁCrawlerRunConfigǁ__init____mutmut_43, 
        'xǁCrawlerRunConfigǁ__init____mutmut_44': xǁCrawlerRunConfigǁ__init____mutmut_44, 
        'xǁCrawlerRunConfigǁ__init____mutmut_45': xǁCrawlerRunConfigǁ__init____mutmut_45, 
        'xǁCrawlerRunConfigǁ__init____mutmut_46': xǁCrawlerRunConfigǁ__init____mutmut_46, 
        'xǁCrawlerRunConfigǁ__init____mutmut_47': xǁCrawlerRunConfigǁ__init____mutmut_47, 
        'xǁCrawlerRunConfigǁ__init____mutmut_48': xǁCrawlerRunConfigǁ__init____mutmut_48, 
        'xǁCrawlerRunConfigǁ__init____mutmut_49': xǁCrawlerRunConfigǁ__init____mutmut_49, 
        'xǁCrawlerRunConfigǁ__init____mutmut_50': xǁCrawlerRunConfigǁ__init____mutmut_50, 
        'xǁCrawlerRunConfigǁ__init____mutmut_51': xǁCrawlerRunConfigǁ__init____mutmut_51, 
        'xǁCrawlerRunConfigǁ__init____mutmut_52': xǁCrawlerRunConfigǁ__init____mutmut_52
    }
    
    def __init__(self, *args, **kwargs):
        result = _mutmut_trampoline(object.__getattribute__(self, "xǁCrawlerRunConfigǁ__init____mutmut_orig"), object.__getattribute__(self, "xǁCrawlerRunConfigǁ__init____mutmut_mutants"), args, kwargs, self)
        return result 
    
    __init__.__signature__ = _mutmut_signature(xǁCrawlerRunConfigǁ__init____mutmut_orig)
    xǁCrawlerRunConfigǁ__init____mutmut_orig.__name__ = 'xǁCrawlerRunConfigǁ__init__'


class JsonXPathExtractionStrategy:
    def xǁJsonXPathExtractionStrategyǁ__init____mutmut_orig(self, *args: Any, **kwargs: Any) -> None:
        self.schema = kwargs.get("schema")
    def xǁJsonXPathExtractionStrategyǁ__init____mutmut_1(self, *args: Any, **kwargs: Any) -> None:
        self.schema = None
    def xǁJsonXPathExtractionStrategyǁ__init____mutmut_2(self, *args: Any, **kwargs: Any) -> None:
        self.schema = kwargs.get(None)
    def xǁJsonXPathExtractionStrategyǁ__init____mutmut_3(self, *args: Any, **kwargs: Any) -> None:
        self.schema = kwargs.get("XXschemaXX")
    def xǁJsonXPathExtractionStrategyǁ__init____mutmut_4(self, *args: Any, **kwargs: Any) -> None:
        self.schema = kwargs.get("SCHEMA")
    
    xǁJsonXPathExtractionStrategyǁ__init____mutmut_mutants : ClassVar[MutantDict] = {
    'xǁJsonXPathExtractionStrategyǁ__init____mutmut_1': xǁJsonXPathExtractionStrategyǁ__init____mutmut_1, 
        'xǁJsonXPathExtractionStrategyǁ__init____mutmut_2': xǁJsonXPathExtractionStrategyǁ__init____mutmut_2, 
        'xǁJsonXPathExtractionStrategyǁ__init____mutmut_3': xǁJsonXPathExtractionStrategyǁ__init____mutmut_3, 
        'xǁJsonXPathExtractionStrategyǁ__init____mutmut_4': xǁJsonXPathExtractionStrategyǁ__init____mutmut_4
    }
    
    def __init__(self, *args, **kwargs):
        result = _mutmut_trampoline(object.__getattribute__(self, "xǁJsonXPathExtractionStrategyǁ__init____mutmut_orig"), object.__getattribute__(self, "xǁJsonXPathExtractionStrategyǁ__init____mutmut_mutants"), args, kwargs, self)
        return result 
    
    __init__.__signature__ = _mutmut_signature(xǁJsonXPathExtractionStrategyǁ__init____mutmut_orig)
    xǁJsonXPathExtractionStrategyǁ__init____mutmut_orig.__name__ = 'xǁJsonXPathExtractionStrategyǁ__init__'

    @staticmethod
    def generate_schema(*args: Any, **kwargs: Any):
        return {}


class LLMConfig:
    def xǁLLMConfigǁ__init____mutmut_orig(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = kwargs.get("api_token")
    def xǁLLMConfigǁ__init____mutmut_1(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = None
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = kwargs.get("api_token")
    def xǁLLMConfigǁ__init____mutmut_2(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get(None)
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = kwargs.get("api_token")
    def xǁLLMConfigǁ__init____mutmut_3(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("XXproviderXX")
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = kwargs.get("api_token")
    def xǁLLMConfigǁ__init____mutmut_4(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("PROVIDER")
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = kwargs.get("api_token")
    def xǁLLMConfigǁ__init____mutmut_5(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = None
        self.api_token: str | None = kwargs.get("api_token")
    def xǁLLMConfigǁ__init____mutmut_6(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = kwargs.get(None)
        self.api_token: str | None = kwargs.get("api_token")
    def xǁLLMConfigǁ__init____mutmut_7(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = kwargs.get("XXbase_urlXX")
        self.api_token: str | None = kwargs.get("api_token")
    def xǁLLMConfigǁ__init____mutmut_8(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = kwargs.get("BASE_URL")
        self.api_token: str | None = kwargs.get("api_token")
    def xǁLLMConfigǁ__init____mutmut_9(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = None
    def xǁLLMConfigǁ__init____mutmut_10(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = kwargs.get(None)
    def xǁLLMConfigǁ__init____mutmut_11(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = kwargs.get("XXapi_tokenXX")
    def xǁLLMConfigǁ__init____mutmut_12(self, *args: Any, **kwargs: Any) -> None:
        self.provider: str | None = kwargs.get("provider")
        self.base_url: str | None = kwargs.get("base_url")
        self.api_token: str | None = kwargs.get("API_TOKEN")
    
    xǁLLMConfigǁ__init____mutmut_mutants : ClassVar[MutantDict] = {
    'xǁLLMConfigǁ__init____mutmut_1': xǁLLMConfigǁ__init____mutmut_1, 
        'xǁLLMConfigǁ__init____mutmut_2': xǁLLMConfigǁ__init____mutmut_2, 
        'xǁLLMConfigǁ__init____mutmut_3': xǁLLMConfigǁ__init____mutmut_3, 
        'xǁLLMConfigǁ__init____mutmut_4': xǁLLMConfigǁ__init____mutmut_4, 
        'xǁLLMConfigǁ__init____mutmut_5': xǁLLMConfigǁ__init____mutmut_5, 
        'xǁLLMConfigǁ__init____mutmut_6': xǁLLMConfigǁ__init____mutmut_6, 
        'xǁLLMConfigǁ__init____mutmut_7': xǁLLMConfigǁ__init____mutmut_7, 
        'xǁLLMConfigǁ__init____mutmut_8': xǁLLMConfigǁ__init____mutmut_8, 
        'xǁLLMConfigǁ__init____mutmut_9': xǁLLMConfigǁ__init____mutmut_9, 
        'xǁLLMConfigǁ__init____mutmut_10': xǁLLMConfigǁ__init____mutmut_10, 
        'xǁLLMConfigǁ__init____mutmut_11': xǁLLMConfigǁ__init____mutmut_11, 
        'xǁLLMConfigǁ__init____mutmut_12': xǁLLMConfigǁ__init____mutmut_12
    }
    
    def __init__(self, *args, **kwargs):
        result = _mutmut_trampoline(object.__getattribute__(self, "xǁLLMConfigǁ__init____mutmut_orig"), object.__getattribute__(self, "xǁLLMConfigǁ__init____mutmut_mutants"), args, kwargs, self)
        return result 
    
    __init__.__signature__ = _mutmut_signature(xǁLLMConfigǁ__init____mutmut_orig)
    xǁLLMConfigǁ__init____mutmut_orig.__name__ = 'xǁLLMConfigǁ__init__'


class AsyncWebCrawler:
    def xǁAsyncWebCrawlerǁ__init____mutmut_orig(self, *args: Any, **kwargs: Any) -> None:
        self.config = kwargs.get("config")
    def xǁAsyncWebCrawlerǁ__init____mutmut_1(self, *args: Any, **kwargs: Any) -> None:
        self.config = None
    def xǁAsyncWebCrawlerǁ__init____mutmut_2(self, *args: Any, **kwargs: Any) -> None:
        self.config = kwargs.get(None)
    def xǁAsyncWebCrawlerǁ__init____mutmut_3(self, *args: Any, **kwargs: Any) -> None:
        self.config = kwargs.get("XXconfigXX")
    def xǁAsyncWebCrawlerǁ__init____mutmut_4(self, *args: Any, **kwargs: Any) -> None:
        self.config = kwargs.get("CONFIG")
    
    xǁAsyncWebCrawlerǁ__init____mutmut_mutants : ClassVar[MutantDict] = {
    'xǁAsyncWebCrawlerǁ__init____mutmut_1': xǁAsyncWebCrawlerǁ__init____mutmut_1, 
        'xǁAsyncWebCrawlerǁ__init____mutmut_2': xǁAsyncWebCrawlerǁ__init____mutmut_2, 
        'xǁAsyncWebCrawlerǁ__init____mutmut_3': xǁAsyncWebCrawlerǁ__init____mutmut_3, 
        'xǁAsyncWebCrawlerǁ__init____mutmut_4': xǁAsyncWebCrawlerǁ__init____mutmut_4
    }
    
    def __init__(self, *args, **kwargs):
        result = _mutmut_trampoline(object.__getattribute__(self, "xǁAsyncWebCrawlerǁ__init____mutmut_orig"), object.__getattribute__(self, "xǁAsyncWebCrawlerǁ__init____mutmut_mutants"), args, kwargs, self)
        return result 
    
    __init__.__signature__ = _mutmut_signature(xǁAsyncWebCrawlerǁ__init____mutmut_orig)
    xǁAsyncWebCrawlerǁ__init____mutmut_orig.__name__ = 'xǁAsyncWebCrawlerǁ__init__'

    async def __aenter__(self) -> "AsyncWebCrawler":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None

    async def xǁAsyncWebCrawlerǁarun__mutmut_orig(self, *args: Any, **kwargs: Any) -> Any:
        return kwargs.get("result")

    async def xǁAsyncWebCrawlerǁarun__mutmut_1(self, *args: Any, **kwargs: Any) -> Any:
        return kwargs.get(None)

    async def xǁAsyncWebCrawlerǁarun__mutmut_2(self, *args: Any, **kwargs: Any) -> Any:
        return kwargs.get("XXresultXX")

    async def xǁAsyncWebCrawlerǁarun__mutmut_3(self, *args: Any, **kwargs: Any) -> Any:
        return kwargs.get("RESULT")
    
    xǁAsyncWebCrawlerǁarun__mutmut_mutants : ClassVar[MutantDict] = {
    'xǁAsyncWebCrawlerǁarun__mutmut_1': xǁAsyncWebCrawlerǁarun__mutmut_1, 
        'xǁAsyncWebCrawlerǁarun__mutmut_2': xǁAsyncWebCrawlerǁarun__mutmut_2, 
        'xǁAsyncWebCrawlerǁarun__mutmut_3': xǁAsyncWebCrawlerǁarun__mutmut_3
    }
    
    def arun(self, *args, **kwargs):
        result = _mutmut_trampoline(object.__getattribute__(self, "xǁAsyncWebCrawlerǁarun__mutmut_orig"), object.__getattribute__(self, "xǁAsyncWebCrawlerǁarun__mutmut_mutants"), args, kwargs, self)
        return result 
    
    arun.__signature__ = _mutmut_signature(xǁAsyncWebCrawlerǁarun__mutmut_orig)
    xǁAsyncWebCrawlerǁarun__mutmut_orig.__name__ = 'xǁAsyncWebCrawlerǁarun'

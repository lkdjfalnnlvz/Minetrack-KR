function $parcel$export(e, n, v, s) {
  Object.defineProperty(e, n, {get: v, set: s, enumerable: true, configurable: true});
}
function $parcel$interopDefault(a) {
  return a && a.__esModule ? a.default : a;
}
var $parcel$global =
typeof globalThis !== 'undefined'
  ? globalThis
  : typeof self !== 'undefined'
  ? self
  : typeof window !== 'undefined'
  ? window
  : typeof global !== 'undefined'
  ? global
  : {};
var $parcel$modules = {};
var $parcel$inits = {};

var parcelRequire = $parcel$global["parcelRequiree05a"];
if (parcelRequire == null) {
  parcelRequire = function(id) {
    if (id in $parcel$modules) {
      return $parcel$modules[id].exports;
    }
    if (id in $parcel$inits) {
      var init = $parcel$inits[id];
      delete $parcel$inits[id];
      var module = {id: id, exports: {}};
      $parcel$modules[id] = module;
      init.call(module.exports, module, module.exports);
      return module.exports;
    }
    var err = new Error("Cannot find module '" + id + "'");
    err.code = 'MODULE_NOT_FOUND';
    throw err;
  };

  parcelRequire.register = function register(id, init) {
    $parcel$inits[id] = init;
  };

  $parcel$global["parcelRequiree05a"] = parcelRequire;
}
parcelRequire.register("h8nox", function(module, exports) {

$parcel$export(module.exports, "getBundleURL", () => $c79554c17d4755de$export$bdfd709ae4826697, (v) => $c79554c17d4755de$export$bdfd709ae4826697 = v);
var $c79554c17d4755de$export$bdfd709ae4826697;
var $c79554c17d4755de$export$c9e73fbda7da57b6;
var $c79554c17d4755de$export$5a759dc7a1cfb72a;
"use strict";
var $c79554c17d4755de$var$bundleURL = {};
function $c79554c17d4755de$var$getBundleURLCached(id) {
    var value = $c79554c17d4755de$var$bundleURL[id];
    if (!value) {
        value = $c79554c17d4755de$var$getBundleURL();
        $c79554c17d4755de$var$bundleURL[id] = value;
    }
    return value;
}
function $c79554c17d4755de$var$getBundleURL() {
    try {
        throw new Error();
    } catch (err) {
        var matches = ("" + err.stack).match(/(https?|file|ftp|(chrome|moz|safari-web)-extension):\/\/[^)\n]+/g);
        if (matches) // The first two stack frames will be this function and getBundleURLCached.
        // Use the 3rd one, which will be a runtime in the original bundle.
        return $c79554c17d4755de$var$getBaseURL(matches[2]);
    }
    return "/";
}
function $c79554c17d4755de$var$getBaseURL(url) {
    return ("" + url).replace(/^((?:https?|file|ftp|(chrome|moz|safari-web)-extension):\/\/.+)\/[^/]+$/, "$1") + "/";
} // TODO: Replace uses with `new URL(url).origin` when ie11 is no longer supported.
function $c79554c17d4755de$var$getOrigin(url) {
    var matches = ("" + url).match(/(https?|file|ftp|(chrome|moz|safari-web)-extension):\/\/[^/]+/);
    if (!matches) throw new Error("Origin not found");
    return matches[0];
}
$c79554c17d4755de$export$bdfd709ae4826697 = $c79554c17d4755de$var$getBundleURLCached;
$c79554c17d4755de$export$c9e73fbda7da57b6 = $c79554c17d4755de$var$getBaseURL;
$c79554c17d4755de$export$5a759dc7a1cfb72a = $c79554c17d4755de$var$getOrigin;

});

/**
* Copyright (c) 2020, Leon Sorokin
* All rights reserved. (MIT Licensed)
*
* uPlot.js (μPlot)
* A small, fast chart for time series, lines, areas, ohlc & bars
* https://github.com/leeoniya/uPlot (v1.0.11)
*/ function $516155acbd03f669$var$debounce(fn, time) {
    let pending = null;
    function run() {
        pending = null;
        fn();
    }
    return function() {
        clearTimeout(pending);
        pending = setTimeout(run, time);
    };
}
// binary search for index of closest value
function $516155acbd03f669$var$closestIdx(num, arr, lo, hi) {
    let mid;
    lo = lo || 0;
    hi = hi || arr.length - 1;
    let bitwise = hi <= 2147483647;
    while(hi - lo > 1){
        mid = bitwise ? lo + hi >> 1 : $516155acbd03f669$var$floor((lo + hi) / 2);
        if (arr[mid] < num) lo = mid;
        else hi = mid;
    }
    if (num - arr[lo] <= arr[hi] - num) return lo;
    return hi;
}
function $516155acbd03f669$var$getMinMax(data, _i0, _i1) {
    //	console.log("getMinMax()");
    let _min = $516155acbd03f669$var$inf;
    let _max = -$516155acbd03f669$var$inf;
    for(let i = _i0; i <= _i1; i++)if (data[i] != null) {
        _min = $516155acbd03f669$var$min(_min, data[i]);
        _max = $516155acbd03f669$var$max(_max, data[i]);
    }
    return [
        _min,
        _max
    ];
}
// this ensures that non-temporal/numeric y-axes get multiple-snapped padding added above/below
// TODO: also account for incrs when snapping to ensure top of axis gets a tick & value
function $516155acbd03f669$var$rangeNum(min, max, mult, extra) {
    // auto-scale Y
    const delta = max - min;
    const mag = $516155acbd03f669$var$log10(delta || $516155acbd03f669$var$abs(max) || 1);
    const exp = $516155acbd03f669$var$floor(mag);
    const incr = $516155acbd03f669$var$pow(10, exp) * mult;
    const buf = delta == 0 ? incr : 0;
    let snappedMin = $516155acbd03f669$var$round6($516155acbd03f669$var$incrRoundDn(min - buf, incr));
    let snappedMax = $516155acbd03f669$var$round6($516155acbd03f669$var$incrRoundUp(max + buf, incr));
    if (extra) {
        // for flat data, always use 0 as one chart extreme & place data in center
        if (delta == 0) {
            if (max > 0) {
                snappedMin = 0;
                snappedMax = max * 2;
            } else if (max < 0) {
                snappedMax = 0;
                snappedMin = min * 2;
            }
        } else {
            // if buffer is too small, increase it
            if (snappedMax - max < incr) snappedMax += incr;
            if (min - snappedMin < incr) snappedMin -= incr;
            // if original data never crosses 0, use 0 as one chart extreme
            if (min >= 0 && snappedMin < 0) snappedMin = 0;
            if (max <= 0 && snappedMax > 0) snappedMax = 0;
        }
    }
    return [
        snappedMin,
        snappedMax
    ];
}
const $516155acbd03f669$var$M = Math;
const $516155acbd03f669$var$abs = $516155acbd03f669$var$M.abs;
const $516155acbd03f669$var$floor = $516155acbd03f669$var$M.floor;
const $516155acbd03f669$var$round = $516155acbd03f669$var$M.round;
const $516155acbd03f669$var$ceil = $516155acbd03f669$var$M.ceil;
const $516155acbd03f669$var$min = $516155acbd03f669$var$M.min;
const $516155acbd03f669$var$max = $516155acbd03f669$var$M.max;
const $516155acbd03f669$var$pow = $516155acbd03f669$var$M.pow;
const $516155acbd03f669$var$log10 = $516155acbd03f669$var$M.log10;
const $516155acbd03f669$var$PI = $516155acbd03f669$var$M.PI;
const $516155acbd03f669$var$inf = Infinity;
function $516155acbd03f669$var$incrRound(num, incr) {
    return $516155acbd03f669$var$round(num / incr) * incr;
}
function $516155acbd03f669$var$clamp(num, _min, _max) {
    return $516155acbd03f669$var$min($516155acbd03f669$var$max(num, _min), _max);
}
function $516155acbd03f669$var$fnOrSelf(v) {
    return typeof v == "function" ? v : ()=>v;
}
function $516155acbd03f669$var$incrRoundUp(num, incr) {
    return $516155acbd03f669$var$ceil(num / incr) * incr;
}
function $516155acbd03f669$var$incrRoundDn(num, incr) {
    return $516155acbd03f669$var$floor(num / incr) * incr;
}
function $516155acbd03f669$var$round3(val) {
    return $516155acbd03f669$var$round(val * 1e3) / 1e3;
}
function $516155acbd03f669$var$round6(val) {
    return $516155acbd03f669$var$round(val * 1e6) / 1e6;
}
//export const assign = Object.assign;
const $516155acbd03f669$var$isArr = Array.isArray;
function $516155acbd03f669$var$isStr(v) {
    return typeof v === "string";
}
function $516155acbd03f669$var$isObj(v) {
    return typeof v === "object" && v !== null;
}
function $516155acbd03f669$var$copy(o) {
    let out;
    if ($516155acbd03f669$var$isArr(o)) out = o.map($516155acbd03f669$var$copy);
    else if ($516155acbd03f669$var$isObj(o)) {
        out = {};
        for(var k in o)out[k] = $516155acbd03f669$var$copy(o[k]);
    } else out = o;
    return out;
}
function $516155acbd03f669$var$assign(targ) {
    let args = arguments;
    for(let i = 1; i < args.length; i++){
        let src = args[i];
        for(let key in src)if ($516155acbd03f669$var$isObj(targ[key])) $516155acbd03f669$var$assign(targ[key], $516155acbd03f669$var$copy(src[key]));
        else targ[key] = $516155acbd03f669$var$copy(src[key]);
    }
    return targ;
}
const $516155acbd03f669$var$WIDTH = "width";
const $516155acbd03f669$var$HEIGHT = "height";
const $516155acbd03f669$var$TOP = "top";
const $516155acbd03f669$var$BOTTOM = "bottom";
const $516155acbd03f669$var$LEFT = "left";
const $516155acbd03f669$var$RIGHT = "right";
const $516155acbd03f669$var$firstChild = "firstChild";
const $516155acbd03f669$var$createElement = "createElement";
const $516155acbd03f669$var$hexBlack = "#000";
const $516155acbd03f669$var$classList = "classList";
const $516155acbd03f669$var$mousemove = "mousemove";
const $516155acbd03f669$var$mousedown = "mousedown";
const $516155acbd03f669$var$mouseup = "mouseup";
const $516155acbd03f669$var$mouseenter = "mouseenter";
const $516155acbd03f669$var$mouseleave = "mouseleave";
const $516155acbd03f669$var$dblclick = "dblclick";
const $516155acbd03f669$var$resize = "resize";
const $516155acbd03f669$var$scroll = "scroll";
const $516155acbd03f669$var$rAF = requestAnimationFrame;
const $516155acbd03f669$var$doc = document;
const $516155acbd03f669$var$win = window;
const $516155acbd03f669$var$pxRatio = devicePixelRatio;
function $516155acbd03f669$var$addClass(el, c) {
    c != null && el[$516155acbd03f669$var$classList].add(c);
}
function $516155acbd03f669$var$remClass(el, c) {
    el[$516155acbd03f669$var$classList].remove(c);
}
function $516155acbd03f669$var$setStylePx(el, name, value) {
    el.style[name] = value + "px";
}
function $516155acbd03f669$var$placeTag(tag, cls, targ, refEl) {
    let el = $516155acbd03f669$var$doc[$516155acbd03f669$var$createElement](tag);
    if (cls != null) $516155acbd03f669$var$addClass(el, cls);
    if (targ != null) targ.insertBefore(el, refEl);
    return el;
}
function $516155acbd03f669$var$placeDiv(cls, targ) {
    return $516155acbd03f669$var$placeTag("div", cls, targ);
}
function $516155acbd03f669$var$trans(el, xPos, yPos) {
    el.style.transform = "translate(" + xPos + "px," + yPos + "px)";
}
const $516155acbd03f669$var$evOpts = {
    passive: true
};
function $516155acbd03f669$var$on(ev, el, cb) {
    el.addEventListener(ev, cb, $516155acbd03f669$var$evOpts);
}
function $516155acbd03f669$var$off(ev, el, cb) {
    el.removeEventListener(ev, cb, $516155acbd03f669$var$evOpts);
}
const $516155acbd03f669$var$months = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December"
];
const $516155acbd03f669$var$days = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday"
];
function $516155acbd03f669$var$slice3(str) {
    return str.slice(0, 3);
}
const $516155acbd03f669$var$days3 = $516155acbd03f669$var$days.map($516155acbd03f669$var$slice3);
const $516155acbd03f669$var$months3 = $516155acbd03f669$var$months.map($516155acbd03f669$var$slice3);
const $516155acbd03f669$var$engNames = {
    MMMM: $516155acbd03f669$var$months,
    MMM: $516155acbd03f669$var$months3,
    WWWW: $516155acbd03f669$var$days,
    WWW: $516155acbd03f669$var$days3
};
function $516155acbd03f669$var$zeroPad2(int) {
    return (int < 10 ? "0" : "") + int;
}
function $516155acbd03f669$var$zeroPad3(int) {
    return (int < 10 ? "00" : int < 100 ? "0" : "") + int;
}
/*
function suffix(int) {
	let mod10 = int % 10;

	return int + (
		mod10 == 1 && int != 11 ? "st" :
		mod10 == 2 && int != 12 ? "nd" :
		mod10 == 3 && int != 13 ? "rd" : "th"
	);
}
*/ const $516155acbd03f669$var$getFullYear = "getFullYear";
const $516155acbd03f669$var$getMonth = "getMonth";
const $516155acbd03f669$var$getDate = "getDate";
const $516155acbd03f669$var$getDay = "getDay";
const $516155acbd03f669$var$getHours = "getHours";
const $516155acbd03f669$var$getMinutes = "getMinutes";
const $516155acbd03f669$var$getSeconds = "getSeconds";
const $516155acbd03f669$var$getMilliseconds = "getMilliseconds";
const $516155acbd03f669$var$subs = {
    // 2019
    YYYY: (d)=>d[$516155acbd03f669$var$getFullYear](),
    // 19
    YY: (d)=>(d[$516155acbd03f669$var$getFullYear]() + "").slice(2),
    // July
    MMMM: (d, names)=>names.MMMM[d[$516155acbd03f669$var$getMonth]()],
    // Jul
    MMM: (d, names)=>names.MMM[d[$516155acbd03f669$var$getMonth]()],
    // 07
    MM: (d)=>$516155acbd03f669$var$zeroPad2(d[$516155acbd03f669$var$getMonth]() + 1),
    // 7
    M: (d)=>d[$516155acbd03f669$var$getMonth]() + 1,
    // 09
    DD: (d)=>$516155acbd03f669$var$zeroPad2(d[$516155acbd03f669$var$getDate]()),
    // 9
    D: (d)=>d[$516155acbd03f669$var$getDate](),
    // Monday
    WWWW: (d, names)=>names.WWWW[d[$516155acbd03f669$var$getDay]()],
    // Mon
    WWW: (d, names)=>names.WWW[d[$516155acbd03f669$var$getDay]()],
    // 03
    HH: (d)=>$516155acbd03f669$var$zeroPad2(d[$516155acbd03f669$var$getHours]()),
    // 3
    H: (d)=>d[$516155acbd03f669$var$getHours](),
    // 9 (12hr, unpadded)
    h: (d)=>{
        let h = d[$516155acbd03f669$var$getHours]();
        return h == 0 ? 12 : h > 12 ? h - 12 : h;
    },
    // AM
    AA: (d)=>d[$516155acbd03f669$var$getHours]() >= 12 ? "PM" : "AM",
    // am
    aa: (d)=>d[$516155acbd03f669$var$getHours]() >= 12 ? "pm" : "am",
    // a
    a: (d)=>d[$516155acbd03f669$var$getHours]() >= 12 ? "p" : "a",
    // 09
    mm: (d)=>$516155acbd03f669$var$zeroPad2(d[$516155acbd03f669$var$getMinutes]()),
    // 9
    m: (d)=>d[$516155acbd03f669$var$getMinutes](),
    // 09
    ss: (d)=>$516155acbd03f669$var$zeroPad2(d[$516155acbd03f669$var$getSeconds]()),
    // 9
    s: (d)=>d[$516155acbd03f669$var$getSeconds](),
    // 374
    fff: (d)=>$516155acbd03f669$var$zeroPad3(d[$516155acbd03f669$var$getMilliseconds]())
};
function $516155acbd03f669$var$fmtDate(tpl, names) {
    names = names || $516155acbd03f669$var$engNames;
    let parts = [];
    let R = /\{([a-z]+)\}|[^{]+/gi, m;
    while(m = R.exec(tpl))parts.push(m[0][0] == "{" ? $516155acbd03f669$var$subs[m[1]] : m[0]);
    return (d)=>{
        let out = "";
        for(let i = 0; i < parts.length; i++)out += typeof parts[i] == "string" ? parts[i] : parts[i](d, names);
        return out;
    };
}
// https://stackoverflow.com/questions/15141762/how-to-initialize-a-javascript-date-to-a-particular-time-zone/53652131#53652131
function $516155acbd03f669$var$tzDate(date, tz) {
    let date2;
    // perf optimization
    if (tz == "Etc/UTC") date2 = new Date(+date + date.getTimezoneOffset() * 6e4);
    else {
        date2 = new Date(date.toLocaleString("en-US", {
            timeZone: tz
        }));
        date2.setMilliseconds(date[$516155acbd03f669$var$getMilliseconds]());
    }
    return date2;
}
//export const series = [];
// default formatters:
function $516155acbd03f669$var$genIncrs(minExp, maxExp, mults) {
    let incrs = [];
    for(let exp = minExp; exp < maxExp; exp++)for(let i = 0; i < mults.length; i++){
        let incr = mults[i] * $516155acbd03f669$var$pow(10, exp);
        incrs.push(+incr.toFixed($516155acbd03f669$var$abs(exp)));
    }
    return incrs;
}
const $516155acbd03f669$var$incrMults = [
    1,
    2,
    5
];
const $516155acbd03f669$var$decIncrs = $516155acbd03f669$var$genIncrs(-12, 0, $516155acbd03f669$var$incrMults);
const $516155acbd03f669$var$intIncrs = $516155acbd03f669$var$genIncrs(0, 12, $516155acbd03f669$var$incrMults);
const $516155acbd03f669$var$numIncrs = $516155acbd03f669$var$decIncrs.concat($516155acbd03f669$var$intIncrs);
let $516155acbd03f669$var$s = 1, $516155acbd03f669$var$m = 60, $516155acbd03f669$var$h = $516155acbd03f669$var$m * $516155acbd03f669$var$m, $516155acbd03f669$var$d = $516155acbd03f669$var$h * 24, $516155acbd03f669$var$mo = $516155acbd03f669$var$d * 30, $516155acbd03f669$var$y = $516155acbd03f669$var$d * 365;
// starting below 1e-3 is a hack to allow the incr finder to choose & bail out at incr < 1ms
const $516155acbd03f669$var$timeIncrs = [
    5e-4
].concat($516155acbd03f669$var$genIncrs(-3, 0, $516155acbd03f669$var$incrMults), [
    // minute divisors (# of secs)
    1,
    5,
    10,
    15,
    30,
    // hour divisors (# of mins)
    $516155acbd03f669$var$m,
    $516155acbd03f669$var$m * 5,
    $516155acbd03f669$var$m * 10,
    $516155acbd03f669$var$m * 15,
    $516155acbd03f669$var$m * 30,
    // day divisors (# of hrs)
    $516155acbd03f669$var$h,
    $516155acbd03f669$var$h * 2,
    $516155acbd03f669$var$h * 3,
    $516155acbd03f669$var$h * 4,
    $516155acbd03f669$var$h * 6,
    $516155acbd03f669$var$h * 8,
    $516155acbd03f669$var$h * 12,
    // month divisors TODO: need more?
    $516155acbd03f669$var$d,
    $516155acbd03f669$var$d * 2,
    $516155acbd03f669$var$d * 3,
    $516155acbd03f669$var$d * 4,
    $516155acbd03f669$var$d * 5,
    $516155acbd03f669$var$d * 6,
    $516155acbd03f669$var$d * 7,
    $516155acbd03f669$var$d * 8,
    $516155acbd03f669$var$d * 9,
    $516155acbd03f669$var$d * 10,
    $516155acbd03f669$var$d * 15,
    // year divisors (# months, approx)
    $516155acbd03f669$var$mo,
    $516155acbd03f669$var$mo * 2,
    $516155acbd03f669$var$mo * 3,
    $516155acbd03f669$var$mo * 4,
    $516155acbd03f669$var$mo * 6,
    // century divisors
    $516155acbd03f669$var$y,
    $516155acbd03f669$var$y * 2,
    $516155acbd03f669$var$y * 5,
    $516155acbd03f669$var$y * 10,
    $516155acbd03f669$var$y * 25,
    $516155acbd03f669$var$y * 50,
    $516155acbd03f669$var$y * 100
]);
function $516155acbd03f669$var$timeAxisStamps(stampCfg, fmtDate) {
    return stampCfg.map((s)=>[
            s[0],
            fmtDate(s[1]),
            s[2],
            fmtDate(s[4] ? s[1] + s[3] : s[3])
        ]);
}
const $516155acbd03f669$var$yyyy = "{YYYY}";
const $516155acbd03f669$var$NLyyyy = "\n" + $516155acbd03f669$var$yyyy;
const $516155acbd03f669$var$md = "{M}/{D}";
const $516155acbd03f669$var$NLmd = "\n" + $516155acbd03f669$var$md;
const $516155acbd03f669$var$aa = "{aa}";
const $516155acbd03f669$var$hmm = "{h}:{mm}";
const $516155acbd03f669$var$hmmaa = $516155acbd03f669$var$hmm + $516155acbd03f669$var$aa;
const $516155acbd03f669$var$ss = ":{ss}";
// [0]: minimum num secs in the tick incr
// [1]: normal tick format
// [2]: when a differing <x> is encountered - 1: sec, 2: min, 3: hour, 4: day, 5: week, 6: month, 7: year
// [3]: use a longer more contextual format
// [4]: modes: 0: replace [1] -> [3], 1: concat [1] + [3]
const $516155acbd03f669$var$_timeAxisStamps = [
    [
        $516155acbd03f669$var$y,
        $516155acbd03f669$var$yyyy,
        7,
        "",
        1
    ],
    [
        $516155acbd03f669$var$d * 28,
        "{MMM}",
        7,
        $516155acbd03f669$var$NLyyyy,
        1
    ],
    [
        $516155acbd03f669$var$d,
        $516155acbd03f669$var$md,
        7,
        $516155acbd03f669$var$NLyyyy,
        1
    ],
    [
        $516155acbd03f669$var$h,
        "{h}" + $516155acbd03f669$var$aa,
        4,
        $516155acbd03f669$var$NLmd,
        1
    ],
    [
        $516155acbd03f669$var$m,
        $516155acbd03f669$var$hmmaa,
        4,
        $516155acbd03f669$var$NLmd,
        1
    ],
    [
        $516155acbd03f669$var$s,
        $516155acbd03f669$var$ss,
        2,
        $516155acbd03f669$var$NLmd + " " + $516155acbd03f669$var$hmmaa,
        1
    ],
    [
        1e-3,
        $516155acbd03f669$var$ss + ".{fff}",
        2,
        $516155acbd03f669$var$NLmd + " " + $516155acbd03f669$var$hmmaa,
        1
    ]
];
// TODO: will need to accept spaces[] and pull incr into the loop when grid will be non-uniform, eg for log scales.
// currently we ignore this for months since they're *nearly* uniform and the added complexity is not worth it
function $516155acbd03f669$var$timeAxisVals(tzDate, stamps) {
    return (self, splits, space, incr)=>{
        let s = stamps.find((e)=>incr >= e[0]) || stamps[stamps.length - 1];
        // these track boundaries when a full label is needed again
        let prevYear = null;
        let prevDate = null;
        let prevMinu = null;
        return splits.map((split, i)=>{
            let date = tzDate(split);
            let newYear = date[$516155acbd03f669$var$getFullYear]();
            let newDate = date[$516155acbd03f669$var$getDate]();
            let newMinu = date[$516155acbd03f669$var$getMinutes]();
            let diffYear = newYear != prevYear;
            let diffDate = newDate != prevDate;
            let diffMinu = newMinu != prevMinu;
            let stamp = s[2] == 7 && diffYear || s[2] == 4 && diffDate || s[2] == 2 && diffMinu ? s[3] : s[1];
            prevYear = newYear;
            prevDate = newDate;
            prevMinu = newMinu;
            return stamp(date);
        });
    };
}
function $516155acbd03f669$var$mkDate(y, m, d) {
    return new Date(y, m, d);
}
// the ensures that axis ticks, values & grid are aligned to logical temporal breakpoints and not an arbitrary timestamp
// https://www.timeanddate.com/time/dst/
// https://www.timeanddate.com/time/dst/2019.html
// https://www.epochconverter.com/timezones
function $516155acbd03f669$var$timeAxisSplits(tzDate) {
    return (self, scaleMin, scaleMax, incr, pctSpace)=>{
        let splits = [];
        let isMo = incr >= $516155acbd03f669$var$mo && incr < $516155acbd03f669$var$y;
        // get the timezone-adjusted date
        let minDate = tzDate(scaleMin);
        let minDateTs = minDate / 1e3;
        // get ts of 12am (this lands us at or before the original scaleMin)
        let minMin = $516155acbd03f669$var$mkDate(minDate[$516155acbd03f669$var$getFullYear](), minDate[$516155acbd03f669$var$getMonth](), isMo ? 1 : minDate[$516155acbd03f669$var$getDate]());
        let minMinTs = minMin / 1e3;
        if (isMo) {
            let moIncr = incr / $516155acbd03f669$var$mo;
            //	let tzOffset = scaleMin - minDateTs;		// needed?
            let split = minDateTs == minMinTs ? minDateTs : $516155acbd03f669$var$mkDate(minMin[$516155acbd03f669$var$getFullYear](), minMin[$516155acbd03f669$var$getMonth]() + moIncr, 1) / 1e3;
            let splitDate = new Date(split * 1e3);
            let baseYear = splitDate[$516155acbd03f669$var$getFullYear]();
            let baseMonth = splitDate[$516155acbd03f669$var$getMonth]();
            for(let i = 0; split <= scaleMax; i++){
                let next = $516155acbd03f669$var$mkDate(baseYear, baseMonth + moIncr * i, 1);
                let offs = next - tzDate(next / 1e3);
                split = (+next + offs) / 1e3;
                if (split <= scaleMax) splits.push(split);
            }
        } else {
            let incr0 = incr >= $516155acbd03f669$var$d ? $516155acbd03f669$var$d : incr;
            let tzOffset = $516155acbd03f669$var$floor(scaleMin) - $516155acbd03f669$var$floor(minDateTs);
            let split1 = minMinTs + tzOffset + $516155acbd03f669$var$incrRoundUp(minDateTs - minMinTs, incr0);
            splits.push(split1);
            let date0 = tzDate(split1);
            let prevHour = date0[$516155acbd03f669$var$getHours]() + date0[$516155acbd03f669$var$getMinutes]() / $516155acbd03f669$var$m + date0[$516155acbd03f669$var$getSeconds]() / $516155acbd03f669$var$h;
            let incrHours = incr / $516155acbd03f669$var$h;
            while(true){
                split1 = $516155acbd03f669$var$round3(split1 + incr);
                let expectedHour = $516155acbd03f669$var$floor($516155acbd03f669$var$round6(prevHour + incrHours)) % 24;
                let splitDate1 = tzDate(split1);
                let actualHour = splitDate1.getHours();
                let dstShift = actualHour - expectedHour;
                if (dstShift > 1) dstShift = -1;
                split1 -= dstShift * $516155acbd03f669$var$h;
                if (split1 > scaleMax) break;
                prevHour = (prevHour + incrHours) % 24;
                // add a tick only if it's further than 70% of the min allowed label spacing
                let prevSplit = splits[splits.length - 1];
                let pctIncr = $516155acbd03f669$var$round3((split1 - prevSplit) / incr);
                if (pctIncr * pctSpace >= .7) splits.push(split1);
            }
        }
        return splits;
    };
}
function $516155acbd03f669$var$timeSeriesStamp(stampCfg, fmtDate) {
    return fmtDate(stampCfg);
}
const $516155acbd03f669$var$_timeSeriesStamp = "{YYYY}-{MM}-{DD} {h}:{mm}{aa}";
function $516155acbd03f669$var$timeSeriesVal(tzDate, stamp) {
    return (self, val)=>stamp(tzDate(val));
}
function $516155acbd03f669$var$cursorPoint(self, si) {
    let s = self.series[si];
    let pt = $516155acbd03f669$var$placeDiv();
    pt.style.background = s.stroke || $516155acbd03f669$var$hexBlack;
    let dia = $516155acbd03f669$var$ptDia(s.width, 1);
    let mar = (dia - 1) / -2;
    $516155acbd03f669$var$setStylePx(pt, $516155acbd03f669$var$WIDTH, dia);
    $516155acbd03f669$var$setStylePx(pt, $516155acbd03f669$var$HEIGHT, dia);
    $516155acbd03f669$var$setStylePx(pt, "marginLeft", mar);
    $516155acbd03f669$var$setStylePx(pt, "marginTop", mar);
    return pt;
}
const $516155acbd03f669$var$cursorOpts = {
    show: true,
    x: true,
    y: true,
    lock: false,
    points: {
        show: $516155acbd03f669$var$cursorPoint
    },
    drag: {
        setScale: true,
        x: true,
        y: false,
        dist: 0,
        uni: null,
        _x: false,
        _y: false
    },
    focus: {
        prox: -1
    },
    locked: false,
    left: -10,
    top: -10,
    idx: null
};
const $516155acbd03f669$var$grid = {
    show: true,
    stroke: "rgba(0,0,0,0.07)",
    width: 2
};
const $516155acbd03f669$var$ticks = $516155acbd03f669$var$assign({}, $516155acbd03f669$var$grid, {
    size: 10
});
const $516155acbd03f669$var$font = '12px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol", "Noto Color Emoji"';
const $516155acbd03f669$var$labelFont = "bold " + $516155acbd03f669$var$font;
const $516155acbd03f669$var$lineMult = 1.5; // font-size multiplier
const $516155acbd03f669$var$xAxisOpts = {
    type: "x",
    show: true,
    scale: "x",
    space: 50,
    gap: 5,
    size: 50,
    labelSize: 30,
    labelFont: $516155acbd03f669$var$labelFont,
    side: 2,
    grid: //	class: "x-vals",
    //	incrs: timeIncrs,
    //	values: timeVals,
    $516155acbd03f669$var$grid,
    ticks: $516155acbd03f669$var$ticks,
    font: $516155acbd03f669$var$font,
    rotate: 0
};
const $516155acbd03f669$var$numSeriesLabel = "Value";
const $516155acbd03f669$var$timeSeriesLabel = "Time";
const $516155acbd03f669$var$xSeriesOpts = {
    show: true,
    scale: "x",
    //	label: "Time",
    //	value: v => stamp(new Date(v * 1e3)),
    // internal caches
    min: $516155acbd03f669$var$inf,
    max: -$516155acbd03f669$var$inf,
    idxs: []
};
// alternative: https://stackoverflow.com/a/2254896
let $516155acbd03f669$var$fmtNum = new Intl.NumberFormat(navigator.language);
function $516155acbd03f669$var$numAxisVals(self, splits, space, incr) {
    return splits.map($516155acbd03f669$var$fmtNum.format);
}
function $516155acbd03f669$var$numAxisSplits(self, scaleMin, scaleMax, incr, pctSpace, forceMin) {
    scaleMin = forceMin ? scaleMin : +$516155acbd03f669$var$incrRoundUp(scaleMin, incr).toFixed(12);
    let splits = [];
    for(let val = scaleMin; val <= scaleMax; val = +(val + incr).toFixed(12))splits.push(val);
    return splits;
}
function $516155acbd03f669$var$numSeriesVal(self, val) {
    return val;
}
const $516155acbd03f669$var$yAxisOpts = {
    type: "y",
    show: true,
    scale: "y",
    space: 40,
    gap: 5,
    size: 50,
    labelSize: 30,
    labelFont: $516155acbd03f669$var$labelFont,
    side: 3,
    grid: //	class: "y-vals",
    //	incrs: numIncrs,
    //	values: (vals, space) => vals,
    $516155acbd03f669$var$grid,
    ticks: $516155acbd03f669$var$ticks,
    font: $516155acbd03f669$var$font,
    rotate: 0
};
// takes stroke width
function $516155acbd03f669$var$ptDia(width, mult) {
    let dia = 3 + (width || 1) * 2;
    return $516155acbd03f669$var$round3(dia * mult);
}
function $516155acbd03f669$var$seriesPoints(self, si) {
    const dia = $516155acbd03f669$var$ptDia(self.series[si].width, $516155acbd03f669$var$pxRatio);
    let maxPts = self.bbox.width / dia / 2;
    let idxs = self.series[0].idxs;
    return idxs[1] - idxs[0] <= maxPts;
}
const $516155acbd03f669$var$ySeriesOpts = {
    //	type: "n",
    scale: "y",
    show: true,
    band: false,
    spanGaps: false,
    alpha: 1,
    points: {
        show: $516155acbd03f669$var$seriesPoints
    },
    //	label: "Value",
    //	value: v => v,
    values: null,
    // internal caches
    min: $516155acbd03f669$var$inf,
    max: -$516155acbd03f669$var$inf,
    idxs: [],
    path: null,
    clip: null
};
const $516155acbd03f669$var$xScaleOpts = {
    time: true,
    auto: false,
    distr: 1,
    min: null,
    max: null
};
const $516155acbd03f669$var$yScaleOpts = $516155acbd03f669$var$assign({}, $516155acbd03f669$var$xScaleOpts, {
    time: false,
    auto: true
});
const $516155acbd03f669$var$syncs = {};
function $516155acbd03f669$var$_sync(opts) {
    let clients = [];
    return {
        sub (client) {
            clients.push(client);
        },
        unsub (client) {
            clients = clients.filter((c)=>c != client);
        },
        pub (type, self, x, y, w, h, i) {
            if (clients.length > 1) clients.forEach((client)=>{
                client != self && client.pub(type, self, x, y, w, h, i);
            });
        }
    };
}
function $516155acbd03f669$var$setDefaults(d, xo, yo, initY) {
    let d2 = initY ? [
        d[0],
        d[1]
    ].concat(d.slice(2)) : [
        d[0]
    ].concat(d.slice(1));
    return d2.map((o, i)=>$516155acbd03f669$var$setDefault(o, i, xo, yo));
}
function $516155acbd03f669$var$setDefault(o, i, xo, yo) {
    return $516155acbd03f669$var$assign({}, i == 0 || o && o.side % 2 == 0 ? xo : yo, o);
}
function $516155acbd03f669$var$getYPos(val, scale, hgt, top) {
    let pctY = (val - scale.min) / (scale.max - scale.min);
    return top + (1 - pctY) * hgt;
}
function $516155acbd03f669$var$getXPos(val, scale, wid, lft) {
    let pctX = (val - scale.min) / (scale.max - scale.min);
    return lft + pctX * wid;
}
function $516155acbd03f669$var$snapTimeX(self, dataMin, dataMax) {
    return [
        dataMin,
        dataMax > dataMin ? dataMax : dataMax + 86400
    ];
}
function $516155acbd03f669$var$snapNumX(self, dataMin, dataMax) {
    const delta = dataMax - dataMin;
    if (delta == 0) {
        const mag = $516155acbd03f669$var$log10(delta || $516155acbd03f669$var$abs(dataMax) || 1);
        const exp = $516155acbd03f669$var$floor(mag) + 1;
        return [
            dataMin,
            $516155acbd03f669$var$incrRoundUp(dataMax, $516155acbd03f669$var$pow(10, exp))
        ];
    } else return [
        dataMin,
        dataMax
    ];
}
// this ensures that non-temporal/numeric y-axes get multiple-snapped padding added above/below
// TODO: also account for incrs when snapping to ensure top of axis gets a tick & value
function $516155acbd03f669$var$snapNumY(self, dataMin, dataMax) {
    return $516155acbd03f669$var$rangeNum(dataMin, dataMax, 0.2, true);
}
// dim is logical (getClientBoundingRect) pixels, not canvas pixels
function $516155acbd03f669$var$findIncr(valDelta, incrs, dim, minSpace) {
    let pxPerUnit = dim / valDelta;
    for(var i = 0; i < incrs.length; i++){
        let space = incrs[i] * pxPerUnit;
        if (space >= minSpace) return [
            incrs[i],
            space
        ];
    }
}
function $516155acbd03f669$var$filtMouse(e) {
    return e.button == 0;
}
function $516155acbd03f669$var$pxRatioFont(font) {
    let fontSize;
    font = font.replace(/\d+/, (m)=>fontSize = $516155acbd03f669$var$round(m * $516155acbd03f669$var$pxRatio));
    return [
        font,
        fontSize
    ];
}
function $516155acbd03f669$var$uPlot(opts, data, then) {
    const self = {};
    const root = self.root = $516155acbd03f669$var$placeDiv("uplot");
    if (opts.id != null) root.id = opts.id;
    $516155acbd03f669$var$addClass(root, opts.class);
    if (opts.title) {
        let title = $516155acbd03f669$var$placeDiv("title", root);
        title.textContent = opts.title;
    }
    const can = $516155acbd03f669$var$placeTag("canvas");
    const ctx = self.ctx = can.getContext("2d");
    const wrap = $516155acbd03f669$var$placeDiv("wrap", root);
    const under = $516155acbd03f669$var$placeDiv("under", wrap);
    wrap.appendChild(can);
    const over = $516155acbd03f669$var$placeDiv("over", wrap);
    opts = $516155acbd03f669$var$copy(opts);
    (opts.plugins || []).forEach((p)=>{
        if (p.opts) opts = p.opts(self, opts) || opts;
    });
    let ready = false;
    const series = self.series = $516155acbd03f669$var$setDefaults(opts.series || [], $516155acbd03f669$var$xSeriesOpts, $516155acbd03f669$var$ySeriesOpts, false);
    const axes = self.axes = $516155acbd03f669$var$setDefaults(opts.axes || [], $516155acbd03f669$var$xAxisOpts, $516155acbd03f669$var$yAxisOpts, true);
    const scales = self.scales = $516155acbd03f669$var$assign({}, {
        x: $516155acbd03f669$var$xScaleOpts,
        y: $516155acbd03f669$var$yScaleOpts
    }, opts.scales);
    const gutters = $516155acbd03f669$var$assign({
        x: $516155acbd03f669$var$round($516155acbd03f669$var$yAxisOpts.size / 2),
        y: $516155acbd03f669$var$round($516155acbd03f669$var$xAxisOpts.size / 3)
    }, opts.gutters);
    //	self.tz = opts.tz || Intl.DateTimeFormat().resolvedOptions().timeZone;
    const _tzDate = opts.tzDate || ((ts)=>new Date(ts * 1e3));
    const _fmtDate = opts.fmtDate || $516155acbd03f669$var$fmtDate;
    const _timeAxisSplits = $516155acbd03f669$var$timeAxisSplits(_tzDate);
    const _timeAxisVals = $516155acbd03f669$var$timeAxisVals(_tzDate, $516155acbd03f669$var$timeAxisStamps($516155acbd03f669$var$_timeAxisStamps, _fmtDate));
    const _timeSeriesVal = $516155acbd03f669$var$timeSeriesVal(_tzDate, $516155acbd03f669$var$timeSeriesStamp($516155acbd03f669$var$_timeSeriesStamp, _fmtDate));
    const pendScales = {};
    // explicitly-set initial scales
    for(let k in scales){
        let sc = scales[k];
        if (sc.min != null || sc.max != null) pendScales[k] = {
            min: sc.min,
            max: sc.max
        };
    }
    const legend = $516155acbd03f669$var$assign({
        show: true
    }, opts.legend);
    const showLegend = legend.show;
    let legendEl;
    let legendRows = [];
    let legendCols;
    let multiValLegend = false;
    if (showLegend) {
        legendEl = $516155acbd03f669$var$placeTag("table", "legend", root);
        const getMultiVals = series[1] ? series[1].values : null;
        multiValLegend = getMultiVals != null;
        if (multiValLegend) {
            let head = $516155acbd03f669$var$placeTag("tr", "labels", legendEl);
            $516155acbd03f669$var$placeTag("th", null, head);
            legendCols = getMultiVals(self, 1, 0);
            for(var key in legendCols)$516155acbd03f669$var$placeTag("th", null, head).textContent = key;
        } else {
            legendCols = {
                _: 0
            };
            $516155acbd03f669$var$addClass(legendEl, "inline");
        }
    }
    function initLegendRow(s, i) {
        if (i == 0 && multiValLegend) return null;
        let _row = [];
        let row = $516155acbd03f669$var$placeTag("tr", "series", legendEl, legendEl.childNodes[i]);
        $516155acbd03f669$var$addClass(row, s.class);
        if (!s.show) $516155acbd03f669$var$addClass(row, "off");
        let label = $516155acbd03f669$var$placeTag("th", null, row);
        let indic = $516155acbd03f669$var$placeDiv("ident", label);
        s.width && (indic.style.borderColor = s.stroke);
        indic.style.backgroundColor = s.fill;
        let text = $516155acbd03f669$var$placeDiv("text", label);
        text.textContent = s.label;
        if (i > 0) {
            $516155acbd03f669$var$on("click", label, (e)=>{
                if (cursor.locked) return;
                $516155acbd03f669$var$filtMouse(e) && setSeries(series.indexOf(s), {
                    show: !s.show
                }, syncOpts.setSeries);
            });
            if (cursorFocus) $516155acbd03f669$var$on($516155acbd03f669$var$mouseenter, label, (e)=>{
                if (cursor.locked) return;
                setSeries(series.indexOf(s), {
                    focus: true
                }, syncOpts.setSeries);
            });
        }
        for(var key in legendCols){
            let v = $516155acbd03f669$var$placeTag("td", null, row);
            v.textContent = "--";
            _row.push(v);
        }
        return _row;
    }
    const cursor = self.cursor = $516155acbd03f669$var$assign({}, $516155acbd03f669$var$cursorOpts, opts.cursor);
    cursor.points.show = $516155acbd03f669$var$fnOrSelf(cursor.points.show);
    const focus = self.focus = $516155acbd03f669$var$assign({}, opts.focus || {
        alpha: 0.3
    }, cursor.focus);
    const cursorFocus = focus.prox >= 0;
    // series-intersection markers
    let cursorPts = [
        null
    ];
    function initCursorPt(s, si) {
        if (si > 0) {
            let pt = cursor.points.show(self, si);
            if (pt) {
                $516155acbd03f669$var$addClass(pt, "cursor-pt");
                $516155acbd03f669$var$addClass(pt, s.class);
                $516155acbd03f669$var$trans(pt, -10, -10);
                over.insertBefore(pt, cursorPts[si]);
                return pt;
            }
        }
    }
    function initSeries(s, i) {
        // init scales & defaults
        const scKey = s.scale;
        const sc = scales[scKey] = $516155acbd03f669$var$assign({}, i == 0 ? $516155acbd03f669$var$xScaleOpts : $516155acbd03f669$var$yScaleOpts, scales[scKey]);
        let isTime = sc.time;
        sc.range = $516155acbd03f669$var$fnOrSelf(sc.range || (isTime ? $516155acbd03f669$var$snapTimeX : i == 0 ? $516155acbd03f669$var$snapNumX : $516155acbd03f669$var$snapNumY));
        let sv = s.value;
        s.value = isTime ? $516155acbd03f669$var$isStr(sv) ? $516155acbd03f669$var$timeSeriesVal(_tzDate, $516155acbd03f669$var$timeSeriesStamp(sv, _fmtDate)) : sv || _timeSeriesVal : sv || $516155acbd03f669$var$numSeriesVal;
        s.label = s.label || (isTime ? $516155acbd03f669$var$timeSeriesLabel : $516155acbd03f669$var$numSeriesLabel);
        if (i > 0) {
            s.width = s.width == null ? 1 : s.width;
            s.paths = s.paths || buildPaths;
            let _ptDia = $516155acbd03f669$var$ptDia(s.width, 1);
            s.points = $516155acbd03f669$var$assign({}, {
                size: _ptDia,
                width: $516155acbd03f669$var$max(1, _ptDia * .2)
            }, s.points);
            s.points.show = $516155acbd03f669$var$fnOrSelf(s.points.show);
            s._paths = null;
        }
        if (showLegend) legendRows.splice(i, 0, initLegendRow(s, i));
        if (cursor.show) {
            let pt = initCursorPt(s, i);
            pt && cursorPts.splice(i, 0, pt);
        }
    }
    function addSeries(opts, si) {
        si = si == null ? series.length : si;
        opts = $516155acbd03f669$var$setDefault(opts, si, $516155acbd03f669$var$xSeriesOpts, $516155acbd03f669$var$ySeriesOpts);
        series.splice(si, 0, opts);
        initSeries(series[si], si);
    }
    self.addSeries = addSeries;
    function delSeries(i) {
        series.splice(i, 1);
        showLegend && legendRows.splice(i, 1)[0][0].parentNode.remove();
        cursorPts.length > 1 && cursorPts.splice(i, 1)[0].remove();
    // TODO: de-init no-longer-needed scales?
    }
    self.delSeries = delSeries;
    series.forEach(initSeries);
    // dependent scales inherit
    for(let k1 in scales){
        let sc1 = scales[k1];
        if (sc1.from != null) scales[k1] = $516155acbd03f669$var$assign({}, scales[sc1.from], sc1);
    }
    const xScaleKey = series[0].scale;
    const xScaleDistr = scales[xScaleKey].distr;
    function initAxis(axis, i) {
        if (axis.show) {
            let isVt = axis.side % 2;
            let sc = scales[axis.scale];
            // this can occur if all series specify non-default scales
            if (sc == null) {
                axis.scale = isVt ? series[1].scale : xScaleKey;
                sc = scales[axis.scale];
            }
            // also set defaults for incrs & values based on axis distr
            let isTime = sc.time;
            axis.space = $516155acbd03f669$var$fnOrSelf(axis.space);
            axis.rotate = $516155acbd03f669$var$fnOrSelf(axis.rotate);
            axis.incrs = $516155acbd03f669$var$fnOrSelf(axis.incrs || (sc.distr == 2 ? $516155acbd03f669$var$intIncrs : isTime ? $516155acbd03f669$var$timeIncrs : $516155acbd03f669$var$numIncrs));
            axis.split = $516155acbd03f669$var$fnOrSelf(axis.split || (isTime && sc.distr == 1 ? _timeAxisSplits : $516155acbd03f669$var$numAxisSplits));
            let av = axis.values;
            axis.values = isTime ? $516155acbd03f669$var$isArr(av) ? $516155acbd03f669$var$timeAxisVals(_tzDate, $516155acbd03f669$var$timeAxisStamps(av, _fmtDate)) : av || _timeAxisVals : av || $516155acbd03f669$var$numAxisVals;
            axis.font = $516155acbd03f669$var$pxRatioFont(axis.font);
            axis.labelFont = $516155acbd03f669$var$pxRatioFont(axis.labelFont);
        }
    }
    // set axis defaults
    axes.forEach(initAxis);
    let dataLen;
    // rendered data window
    let i0 = null;
    let i1 = null;
    const idxs = series[0].idxs;
    let data0 = null;
    function setData(_data, _resetScales) {
        _data = _data || [];
        _data[0] = _data[0] || [];
        self.data = _data;
        data = _data.slice();
        data0 = data[0];
        dataLen = data0.length;
        if (xScaleDistr == 2) data[0] = data0.map((v, i)=>i);
        resetYSeries();
        fire("setData");
        _resetScales !== false && autoScaleX();
    }
    self.setData = setData;
    function autoScaleX() {
        i0 = idxs[0] = 0;
        i1 = idxs[1] = dataLen - 1;
        let _min = xScaleDistr == 2 ? i0 : data[0][i0], _max = xScaleDistr == 2 ? i1 : data[0][i1];
        _setScale(xScaleKey, _min, _max);
    }
    function setCtxStyle(stroke, width, dash, fill) {
        ctx.strokeStyle = stroke || $516155acbd03f669$var$hexBlack;
        ctx.lineWidth = width;
        ctx.lineJoin = "round";
        ctx.setLineDash(dash || []);
        ctx.fillStyle = fill || $516155acbd03f669$var$hexBlack;
    }
    let fullWidCss;
    let fullHgtCss;
    let plotWidCss;
    let plotHgtCss;
    // plot margins to account for axes
    let plotLftCss;
    let plotTopCss;
    let plotLft;
    let plotTop;
    let plotWid;
    let plotHgt;
    self.bbox = {};
    function _setSize(width, height) {
        self.width = fullWidCss = plotWidCss = width;
        self.height = fullHgtCss = plotHgtCss = height;
        plotLftCss = plotTopCss = 0;
        calcPlotRect();
        calcAxesRects();
        let bb = self.bbox;
        plotLft = bb[$516155acbd03f669$var$LEFT] = $516155acbd03f669$var$incrRound(plotLftCss * $516155acbd03f669$var$pxRatio, 0.5);
        plotTop = bb[$516155acbd03f669$var$TOP] = $516155acbd03f669$var$incrRound(plotTopCss * $516155acbd03f669$var$pxRatio, 0.5);
        plotWid = bb[$516155acbd03f669$var$WIDTH] = $516155acbd03f669$var$incrRound(plotWidCss * $516155acbd03f669$var$pxRatio, 0.5);
        plotHgt = bb[$516155acbd03f669$var$HEIGHT] = $516155acbd03f669$var$incrRound(plotHgtCss * $516155acbd03f669$var$pxRatio, 0.5);
        $516155acbd03f669$var$setStylePx(under, $516155acbd03f669$var$LEFT, plotLftCss);
        $516155acbd03f669$var$setStylePx(under, $516155acbd03f669$var$TOP, plotTopCss);
        $516155acbd03f669$var$setStylePx(under, $516155acbd03f669$var$WIDTH, plotWidCss);
        $516155acbd03f669$var$setStylePx(under, $516155acbd03f669$var$HEIGHT, plotHgtCss);
        $516155acbd03f669$var$setStylePx(over, $516155acbd03f669$var$LEFT, plotLftCss);
        $516155acbd03f669$var$setStylePx(over, $516155acbd03f669$var$TOP, plotTopCss);
        $516155acbd03f669$var$setStylePx(over, $516155acbd03f669$var$WIDTH, plotWidCss);
        $516155acbd03f669$var$setStylePx(over, $516155acbd03f669$var$HEIGHT, plotHgtCss);
        $516155acbd03f669$var$setStylePx(wrap, $516155acbd03f669$var$WIDTH, fullWidCss);
        $516155acbd03f669$var$setStylePx(wrap, $516155acbd03f669$var$HEIGHT, fullHgtCss);
        can[$516155acbd03f669$var$WIDTH] = $516155acbd03f669$var$round(fullWidCss * $516155acbd03f669$var$pxRatio);
        can[$516155acbd03f669$var$HEIGHT] = $516155acbd03f669$var$round(fullHgtCss * $516155acbd03f669$var$pxRatio);
        syncRect();
        ready && _setScale(xScaleKey, scales[xScaleKey].min, scales[xScaleKey].max);
        ready && fire("setSize");
    }
    function setSize({ width: width , height: height  }) {
        _setSize(width, height);
    }
    self.setSize = setSize;
    // accumulate axis offsets, reduce canvas width
    function calcPlotRect() {
        // easements for edge labels
        let hasTopAxis = false;
        let hasBtmAxis = false;
        let hasRgtAxis = false;
        let hasLftAxis = false;
        axes.forEach((axis, i)=>{
            if (axis.show) {
                let { side: side , size: size  } = axis;
                let isVt = side % 2;
                let labelSize = axis.labelSize = axis.label != null ? axis.labelSize || 30 : 0;
                let fullSize = size + labelSize;
                if (fullSize > 0) {
                    if (isVt) {
                        plotWidCss -= fullSize;
                        if (side == 3) {
                            plotLftCss += fullSize;
                            hasLftAxis = true;
                        } else hasRgtAxis = true;
                    } else {
                        plotHgtCss -= fullSize;
                        if (side == 0) {
                            plotTopCss += fullSize;
                            hasTopAxis = true;
                        } else hasBtmAxis = true;
                    }
                }
            }
        });
        // hz gutters
        if (hasTopAxis || hasBtmAxis) {
            if (!hasRgtAxis) plotWidCss -= gutters.x;
            if (!hasLftAxis) {
                plotWidCss -= gutters.x;
                plotLftCss += gutters.x;
            }
        }
        // vt gutters
        if (hasLftAxis || hasRgtAxis) {
            if (!hasBtmAxis) plotHgtCss -= gutters.y;
            if (!hasTopAxis) {
                plotHgtCss -= gutters.y;
                plotTopCss += gutters.y;
            }
        }
    }
    function calcAxesRects() {
        // will accum +
        let off1 = plotLftCss + plotWidCss;
        let off2 = plotTopCss + plotHgtCss;
        // will accum -
        let off3 = plotLftCss;
        let off0 = plotTopCss;
        function incrOffset(side, size) {
            switch(side){
                case 1:
                    off1 += size;
                    return off1 - size;
                case 2:
                    off2 += size;
                    return off2 - size;
                case 3:
                    off3 -= size;
                    return off3 + size;
                case 0:
                    off0 -= size;
                    return off0 + size;
            }
        }
        axes.forEach((axis, i)=>{
            let side = axis.side;
            axis._pos = incrOffset(side, axis.size);
            if (axis.label != null) axis._lpos = incrOffset(side, axis.labelSize);
        });
    }
    function setScales() {
        if (inBatch) {
            shouldSetScales = true;
            return;
        }
        //	log("setScales()", arguments);
        if (dataLen > 0) {
            // wip scales
            let wipScales = $516155acbd03f669$var$copy(scales);
            for(let k in wipScales){
                let wsc = wipScales[k];
                let psc = pendScales[k];
                if (psc != null) {
                    $516155acbd03f669$var$assign(wsc, psc);
                    // explicitly setting the x-scale invalidates everything (acts as redraw)
                    if (k == xScaleKey) resetYSeries();
                } else if (k != xScaleKey) {
                    wsc.min = $516155acbd03f669$var$inf;
                    wsc.max = -$516155acbd03f669$var$inf;
                }
            }
            // pre-range y-scales from y series' data values
            series.forEach((s, i)=>{
                let k = s.scale;
                let wsc = wipScales[k];
                // setting the x scale invalidates everything
                if (i == 0) {
                    let minMax = wsc.range(self, wsc.min, wsc.max);
                    wsc.min = minMax[0];
                    wsc.max = minMax[1];
                    i0 = $516155acbd03f669$var$closestIdx(wsc.min, data[0]);
                    i1 = $516155acbd03f669$var$closestIdx(wsc.max, data[0]);
                    // closest indices can be outside of view
                    if (data[0][i0] < wsc.min) i0++;
                    if (data[0][i1] > wsc.max) i1--;
                    s.min = data0[i0];
                    s.max = data0[i1];
                } else if (s.show && pendScales[k] == null) {
                    // only run getMinMax() for invalidated series data, else reuse
                    let minMax1 = s.min == $516155acbd03f669$var$inf ? wsc.auto ? $516155acbd03f669$var$getMinMax(data[i], i0, i1) : [
                        0,
                        100
                    ] : [
                        s.min,
                        s.max
                    ];
                    // initial min/max
                    wsc.min = $516155acbd03f669$var$min(wsc.min, s.min = minMax1[0]);
                    wsc.max = $516155acbd03f669$var$max(wsc.max, s.max = minMax1[1]);
                }
                s.idxs[0] = i0;
                s.idxs[1] = i1;
            });
            // range independent scales
            for(let k1 in wipScales){
                let wsc1 = wipScales[k1];
                if (wsc1.from == null && wsc1.min != $516155acbd03f669$var$inf && pendScales[k1] == null) {
                    let minMax = wsc1.range(self, wsc1.min, wsc1.max);
                    wsc1.min = minMax[0];
                    wsc1.max = minMax[1];
                }
            }
            // range dependent scales
            for(let k2 in wipScales){
                let wsc2 = wipScales[k2];
                if (wsc2.from != null) {
                    let base = wipScales[wsc2.from];
                    if (base.min != $516155acbd03f669$var$inf) {
                        let minMax1 = wsc2.range(self, base.min, base.max);
                        wsc2.min = minMax1[0];
                        wsc2.max = minMax1[1];
                    }
                }
            }
            let changed = {};
            for(let k3 in wipScales){
                let wsc3 = wipScales[k3];
                let sc = scales[k3];
                if (sc.min != wsc3.min || sc.max != wsc3.max) {
                    sc.min = wsc3.min;
                    sc.max = wsc3.max;
                    changed[k3] = true;
                }
            }
            // invalidate paths of all series on changed scales
            series.forEach((s)=>{
                if (changed[s.scale]) s._paths = null;
            });
            for(let k4 in changed)fire("setScale", k4);
        }
        for(let k5 in pendScales)pendScales[k5] = null;
        cursor.show && updateCursor();
    }
    // TODO: drawWrap(si, drawPoints) (save, restore, translate, clip)
    function drawPoints(si) {
        //	log("drawPoints()", arguments);
        let s = series[si];
        let p = s.points;
        const width = $516155acbd03f669$var$round3(p.width * $516155acbd03f669$var$pxRatio);
        const offset = width % 2 / 2;
        const isStroked = p.width > 0;
        let rad = (p.size - p.width) / 2 * $516155acbd03f669$var$pxRatio;
        let dia = $516155acbd03f669$var$round3(rad * 2);
        ctx.translate(offset, offset);
        ctx.save();
        ctx.beginPath();
        ctx.rect(plotLft - dia, plotTop - dia, plotWid + dia * 2, plotHgt + dia * 2);
        ctx.clip();
        ctx.globalAlpha = s.alpha;
        const path = new Path2D();
        for(let pi = i0; pi <= i1; pi++)if (data[si][pi] != null) {
            let x = $516155acbd03f669$var$round($516155acbd03f669$var$getXPos(data[0][pi], scales[xScaleKey], plotWid, plotLft));
            let y = $516155acbd03f669$var$round($516155acbd03f669$var$getYPos(data[si][pi], scales[s.scale], plotHgt, plotTop));
            path.moveTo(x + rad, y);
            path.arc(x, y, rad, 0, $516155acbd03f669$var$PI * 2);
        }
        setCtxStyle(p.stroke || s.stroke || $516155acbd03f669$var$hexBlack, width, null, p.fill || (isStroked ? "#fff" : s.stroke || $516155acbd03f669$var$hexBlack));
        ctx.fill(path);
        isStroked && ctx.stroke(path);
        ctx.globalAlpha = 1;
        ctx.restore();
        ctx.translate(-offset, -offset);
    }
    // grabs the nearest indices with y data outside of x-scale limits
    function getOuterIdxs(ydata) {
        let _i0 = $516155acbd03f669$var$clamp(i0 - 1, 0, dataLen - 1);
        let _i1 = $516155acbd03f669$var$clamp(i1 + 1, 0, dataLen - 1);
        while(ydata[_i0] == null && _i0 > 0)_i0--;
        while(ydata[_i1] == null && _i1 < dataLen - 1)_i1++;
        return [
            _i0,
            _i1
        ];
    }
    let dir = 1;
    function drawSeries() {
        // path building loop must be before draw loop to ensure that all bands are fully constructed
        series.forEach((s, i)=>{
            if (i > 0 && s.show && dataLen > 0 && s._paths == null) {
                let _idxs = getOuterIdxs(data[i]);
                s._paths = s.paths(self, i, _idxs[0], _idxs[1]);
            }
        });
        series.forEach((s, i)=>{
            if (i > 0 && s.show) {
                if (s._paths) drawPath(i);
                if (s.points.show(self, i, i0, i1)) drawPoints(i);
                fire("drawSeries", i);
            }
        });
    }
    function drawPath(si) {
        const s = series[si];
        if (dir == 1) {
            const { stroke: stroke , fill: fill , clip: clip  } = s._paths;
            const width = $516155acbd03f669$var$round3(s[$516155acbd03f669$var$WIDTH] * $516155acbd03f669$var$pxRatio);
            const offset = width % 2 / 2;
            setCtxStyle(s.stroke, width, s.dash, s.fill);
            ctx.globalAlpha = s.alpha;
            ctx.translate(offset, offset);
            ctx.save();
            let lft = plotLft, top = plotTop, wid = plotWid, hgt = plotHgt;
            let halfWid = width * $516155acbd03f669$var$pxRatio / 2;
            if (s.min == 0) hgt += halfWid;
            if (s.max == 0) {
                top -= halfWid;
                hgt += halfWid;
            }
            ctx.beginPath();
            ctx.rect(lft, top, wid, hgt);
            ctx.clip();
            if (clip != null) ctx.clip(clip);
            if (s.band) {
                ctx.fill(stroke);
                width && ctx.stroke(stroke);
            } else {
                width && ctx.stroke(stroke);
                if (s.fill != null) ctx.fill(fill);
            }
            ctx.restore();
            ctx.translate(-offset, -offset);
            ctx.globalAlpha = 1;
        }
        if (s.band) dir *= -1;
    }
    function buildClip(is, gaps, nullHead, nullTail) {
        let s = series[is];
        let clip = null;
        // create clip path (invert gaps and non-gaps)
        if (gaps.length > 0) {
            if (s.spanGaps) {
                let headGap = gaps[0];
                let tailGap = gaps[gaps.length - 1];
                gaps = [];
                if (nullHead) gaps.push(headGap);
                if (nullTail) gaps.push(tailGap);
            }
            clip = new Path2D();
            let prevGapEnd = plotLft;
            for(let i = 0; i < gaps.length; i++){
                let g = gaps[i];
                clip.rect(prevGapEnd, plotTop, g[0] - prevGapEnd, plotTop + plotHgt);
                prevGapEnd = g[1];
            }
            clip.rect(prevGapEnd, plotTop, plotLft + plotWid - prevGapEnd, plotTop + plotHgt);
        }
        return clip;
    }
    function addGap(gaps, outX, x) {
        let prevGap = gaps[gaps.length - 1];
        if (prevGap && prevGap[0] == outX) prevGap[1] = x;
        else gaps.push([
            outX,
            x
        ]);
    }
    function buildPaths(self, is, _i0, _i1) {
        const s = series[is];
        const xdata = data[0];
        const ydata = data[is];
        const scaleX = scales[xScaleKey];
        const scaleY = scales[s.scale];
        const _paths = dir == 1 ? {
            stroke: new Path2D(),
            fill: null,
            clip: null
        } : series[is - 1]._paths;
        const stroke = _paths.stroke;
        const width = $516155acbd03f669$var$round3(s[$516155acbd03f669$var$WIDTH] * $516155acbd03f669$var$pxRatio);
        let minY = $516155acbd03f669$var$inf, maxY = -$516155acbd03f669$var$inf, outY, outX;
        // todo: don't build gaps on dir = -1 pass
        let gaps = [];
        let accX = $516155acbd03f669$var$round($516155acbd03f669$var$getXPos(xdata[dir == 1 ? _i0 : _i1], scaleX, plotWid, plotLft));
        // the moves the shape edge outside the canvas so stroke doesnt bleed in
        if (s.band && dir == 1 && _i0 == i0) {
            if (width) stroke.lineTo(-width, $516155acbd03f669$var$round($516155acbd03f669$var$getYPos(ydata[_i0], scaleY, plotHgt, plotTop)));
            if (scaleX.min < xdata[0]) gaps.push([
                plotLft,
                accX - 1
            ]);
        }
        for(let i = dir == 1 ? _i0 : _i1; i >= _i0 && i <= _i1; i += dir){
            let x = $516155acbd03f669$var$round($516155acbd03f669$var$getXPos(xdata[i], scaleX, plotWid, plotLft));
            if (x == accX) {
                if (ydata[i] != null) {
                    outY = $516155acbd03f669$var$round($516155acbd03f669$var$getYPos(ydata[i], scaleY, plotHgt, plotTop));
                    minY = $516155acbd03f669$var$min(outY, minY);
                    maxY = $516155acbd03f669$var$max(outY, maxY);
                }
            } else {
                let _addGap = false;
                if (minY != $516155acbd03f669$var$inf) {
                    stroke.lineTo(accX, minY);
                    stroke.lineTo(accX, maxY);
                    stroke.lineTo(accX, outY);
                    outX = accX;
                } else _addGap = true;
                if (ydata[i] != null) {
                    outY = $516155acbd03f669$var$round($516155acbd03f669$var$getYPos(ydata[i], scaleY, plotHgt, plotTop));
                    stroke.lineTo(x, outY);
                    minY = maxY = outY;
                    // prior pixel can have data but still start a gap if ends with null
                    if (x - accX > 1 && ydata[i - 1] == null) _addGap = true;
                } else {
                    minY = $516155acbd03f669$var$inf;
                    maxY = -$516155acbd03f669$var$inf;
                }
                _addGap && addGap(gaps, outX, x);
                accX = x;
            }
        }
        // extend or insert rightmost gap if no data exists to the right
        if (ydata[_i1] == null) addGap(gaps, outX, accX);
        if (s.band) {
            let overShoot = width * 100, _iy, _x;
            // the moves the shape edge outside the canvas so stroke doesnt bleed in
            if (dir == -1 && _i0 == i0) {
                _x = plotLft - overShoot;
                _iy = _i0;
            }
            if (dir == 1 && _i1 == i1) {
                _x = plotLft + plotWid + overShoot;
                _iy = _i1;
                if (scaleX.max > xdata[dataLen - 1]) gaps.push([
                    accX,
                    plotLft + plotWid
                ]);
            }
            stroke.lineTo(_x, $516155acbd03f669$var$round($516155acbd03f669$var$getYPos(ydata[_iy], scaleY, plotHgt, plotTop)));
        }
        if (dir == 1) {
            _paths.clip = buildClip(is, gaps, ydata[_i0] == null, ydata[_i1] == null);
            if (s.fill != null) {
                let fill = _paths.fill = new Path2D(stroke);
                let zeroY = $516155acbd03f669$var$round($516155acbd03f669$var$getYPos(0, scaleY, plotHgt, plotTop));
                fill.lineTo(plotLft + plotWid, zeroY);
                fill.lineTo(plotLft, zeroY);
            }
        }
        if (s.band) dir *= -1;
        return _paths;
    }
    function getIncrSpace(axis, min, max, fullDim) {
        let incrSpace;
        if (fullDim <= 0) incrSpace = [
            0,
            0
        ];
        else {
            let minSpace = axis.space(self, min, max, fullDim);
            let incrs = axis.incrs(self, min, max, fullDim, minSpace);
            incrSpace = $516155acbd03f669$var$findIncr(max - min, incrs, fullDim, minSpace);
            incrSpace.push(incrSpace[1] / minSpace);
        }
        return incrSpace;
    }
    function drawOrthoLines(offs, ori, side, pos0, len, width, stroke, dash) {
        let offset = width % 2 / 2;
        ctx.translate(offset, offset);
        setCtxStyle(stroke, width, dash);
        ctx.beginPath();
        let x0, y0, x1, y1, pos1 = pos0 + (side == 0 || side == 3 ? -len : len);
        if (ori == 0) {
            y0 = pos0;
            y1 = pos1;
        } else {
            x0 = pos0;
            x1 = pos1;
        }
        offs.forEach((off, i)=>{
            if (ori == 0) x0 = x1 = off;
            else y0 = y1 = off;
            ctx.moveTo(x0, y0);
            ctx.lineTo(x1, y1);
        });
        ctx.stroke();
        ctx.translate(-offset, -offset);
    }
    function drawAxesGrid() {
        axes.forEach((axis, i)=>{
            if (!axis.show) return;
            let scale = scales[axis.scale];
            // this will happen if all series using a specific scale are toggled off
            if (scale.min == $516155acbd03f669$var$inf) return;
            let side = axis.side;
            let ori = side % 2;
            let { min: min , max: max  } = scale;
            let [incr, space, pctSpace] = getIncrSpace(axis, min, max, ori == 0 ? plotWidCss : plotHgtCss);
            // if we're using index positions, force first tick to match passed index
            let forceMin = scale.distr == 2;
            let splits = axis.split(self, min, max, incr, pctSpace, forceMin);
            let getPos = ori == 0 ? $516155acbd03f669$var$getXPos : $516155acbd03f669$var$getYPos;
            let plotDim = ori == 0 ? plotWid : plotHgt;
            let plotOff = ori == 0 ? plotLft : plotTop;
            let canOffs = splits.map((val)=>$516155acbd03f669$var$round(getPos(val, scale, plotDim, plotOff)));
            let axisGap = $516155acbd03f669$var$round(axis.gap * $516155acbd03f669$var$pxRatio);
            let ticks = axis.ticks;
            let tickSize = ticks.show ? $516155acbd03f669$var$round(ticks.size * $516155acbd03f669$var$pxRatio) : 0;
            // tick labels
            // BOO this assumes a specific data/series
            let values = axis.values(self, scale.distr == 2 ? splits.map((i)=>data0[i]) : splits, space, scale.distr == 2 ? data0[splits[1]] - data0[splits[0]] : incr);
            // rotating of labels only supported on bottom x axis
            let angle = side == 2 ? axis.rotate(self, values, space) * -$516155acbd03f669$var$PI / 180 : 0;
            let basePos = $516155acbd03f669$var$round(axis._pos * $516155acbd03f669$var$pxRatio);
            let shiftAmt = tickSize + axisGap;
            let shiftDir = ori == 0 && side == 0 || ori == 1 && side == 3 ? -1 : 1;
            let finalPos = basePos + shiftAmt * shiftDir;
            let y = ori == 0 ? finalPos : 0;
            let x = ori == 1 ? finalPos : 0;
            ctx.font = axis.font[0];
            ctx.fillStyle = axis.stroke || $516155acbd03f669$var$hexBlack; // rgba?
            ctx.textAlign = angle > 0 ? $516155acbd03f669$var$LEFT : angle < 0 ? $516155acbd03f669$var$RIGHT : ori == 0 ? "center" : side == 3 ? $516155acbd03f669$var$RIGHT : $516155acbd03f669$var$LEFT;
            ctx.textBaseline = angle || ori == 1 ? "middle" : side == 2 ? $516155acbd03f669$var$TOP : $516155acbd03f669$var$BOTTOM;
            let lineHeight = axis.font[1] * $516155acbd03f669$var$lineMult;
            values.forEach((val, i)=>{
                if (ori == 0) x = canOffs[i];
                else y = canOffs[i];
                ("" + val).split(/\n/gm).forEach((text, j)=>{
                    if (angle) {
                        ctx.save();
                        ctx.translate(x, y + j * lineHeight);
                        ctx.rotate(angle);
                        ctx.fillText(text, 0, 0);
                        ctx.restore();
                    } else ctx.fillText(text, x, y + j * lineHeight);
                });
            });
            // axis label
            if (axis.label) {
                ctx.save();
                let baseLpos = $516155acbd03f669$var$round(axis._lpos * $516155acbd03f669$var$pxRatio);
                if (ori == 1) {
                    x = y = 0;
                    ctx.translate(baseLpos, $516155acbd03f669$var$round(plotTop + plotHgt / 2));
                    ctx.rotate((side == 3 ? -$516155acbd03f669$var$PI : $516155acbd03f669$var$PI) / 2);
                } else {
                    x = $516155acbd03f669$var$round(plotLft + plotWid / 2);
                    y = baseLpos;
                }
                ctx.font = axis.labelFont[0];
                //	ctx.fillStyle    = axis.labelStroke || hexBlack;						// rgba?
                ctx.textAlign = "center";
                ctx.textBaseline = side == 2 ? $516155acbd03f669$var$TOP : $516155acbd03f669$var$BOTTOM;
                ctx.fillText(axis.label, x, y);
                ctx.restore();
            }
            // ticks
            if (ticks.show) drawOrthoLines(canOffs, ori, side, basePos, tickSize, $516155acbd03f669$var$round3(ticks[$516155acbd03f669$var$WIDTH] * $516155acbd03f669$var$pxRatio), ticks.stroke);
            // grid
            let grid = axis.grid;
            if (grid.show) drawOrthoLines(canOffs, ori, ori == 0 ? 2 : 1, ori == 0 ? plotTop : plotLft, ori == 0 ? plotHgt : plotWid, $516155acbd03f669$var$round3(grid[$516155acbd03f669$var$WIDTH] * $516155acbd03f669$var$pxRatio), grid.stroke, grid.dash);
        });
        fire("drawAxes");
    }
    function resetYSeries() {
        //	log("resetYSeries()", arguments);
        series.forEach((s, i)=>{
            if (i > 0) {
                s.min = $516155acbd03f669$var$inf;
                s.max = -$516155acbd03f669$var$inf;
                s._paths = null;
            }
        });
    }
    let didPaint;
    function paint() {
        if (inBatch) {
            shouldPaint = true;
            return;
        }
        //	log("paint()", arguments);
        ctx.clearRect(0, 0, can[$516155acbd03f669$var$WIDTH], can[$516155acbd03f669$var$HEIGHT]);
        fire("drawClear");
        drawAxesGrid();
        drawSeries();
        didPaint = true;
        fire("draw");
    }
    self.redraw = (rebuildPaths)=>{
        if (rebuildPaths !== false) _setScale(xScaleKey, scales[xScaleKey].min, scales[xScaleKey].max);
        else paint();
    };
    // redraw() => setScale('x', scales.x.min, scales.x.max);
    // explicit, never re-ranged (is this actually true? for x and y)
    function setScale(key, opts) {
        let sc = scales[key];
        if (sc.from == null) {
            if (key == xScaleKey) {
                if (sc.distr == 2) {
                    opts.min = $516155acbd03f669$var$closestIdx(opts.min, data[0]);
                    opts.max = $516155acbd03f669$var$closestIdx(opts.max, data[0]);
                }
                // prevent setting a temporal x scale too small since Date objects cannot advance ticks smaller than 1ms
                if (sc.time && axes[0].show && opts.max > opts.min) {
                    // since scales and axes are loosly coupled, we have to make some assumptions here :(
                    let incr = getIncrSpace(axes[0], opts.min, opts.max, plotWidCss)[0];
                    if (incr < 1e-3) return;
                }
            }
            //	log("setScale()", arguments);
            pendScales[key] = opts;
            didPaint = false;
            setScales();
            !didPaint && paint();
            didPaint = false;
        }
    }
    self.setScale = setScale;
    //	INTERACTION
    let vt;
    let hz;
    // starting position
    let mouseLeft0;
    let mouseTop0;
    // current position
    let mouseLeft1;
    let mouseTop1;
    let dragging = false;
    const drag = cursor.drag;
    let dragX = drag.x;
    let dragY = drag.y;
    if (cursor.show) {
        let c = "cursor-";
        if (cursor.x) {
            mouseLeft1 = cursor.left;
            vt = $516155acbd03f669$var$placeDiv(c + "x", over);
        }
        if (cursor.y) {
            mouseTop1 = cursor.top;
            hz = $516155acbd03f669$var$placeDiv(c + "y", over);
        }
    }
    const select = self.select = $516155acbd03f669$var$assign({
        show: true,
        left: 0,
        width: 0,
        top: 0,
        height: 0
    }, opts.select);
    const selectDiv = select.show ? $516155acbd03f669$var$placeDiv("select", over) : null;
    function setSelect(opts, _fire) {
        if (select.show) {
            for(let prop in opts)$516155acbd03f669$var$setStylePx(selectDiv, prop, select[prop] = opts[prop]);
            _fire !== false && fire("setSelect");
        }
    }
    self.setSelect = setSelect;
    function toggleDOM(i, onOff) {
        let s = series[i];
        let label = showLegend ? legendRows[i][0].parentNode : null;
        if (s.show) label && $516155acbd03f669$var$remClass(label, "off");
        else {
            label && $516155acbd03f669$var$addClass(label, "off");
            cursorPts.length > 1 && $516155acbd03f669$var$trans(cursorPts[i], 0, -10);
        }
    }
    function _setScale(key, min, max) {
        setScale(key, {
            min: min,
            max: max
        });
    }
    function setSeries(i, opts, pub) {
        //	log("setSeries()", arguments);
        let s = series[i];
        //	batch(() => {
        // will this cause redundant paint() if both show and focus are set?
        if (opts.focus != null) setFocus(i);
        if (opts.show != null) {
            s.show = opts.show;
            toggleDOM(i, opts.show);
            if (s.band) {
                // not super robust, will break if two bands are adjacent
                let ip = series[i + 1] && series[i + 1].band ? i + 1 : i - 1;
                series[ip].show = s.show;
                toggleDOM(ip, opts.show);
            }
            _setScale(xScaleKey, scales[xScaleKey].min, scales[xScaleKey].max); // redraw
        }
        //	});
        // firing setSeries after setScale seems out of order, but provides access to the updated props
        // could improve by predefining firing order and building a queue
        fire("setSeries", i, opts);
        pub && sync.pub("setSeries", self, i, opts);
    }
    self.setSeries = setSeries;
    function _alpha(i, value) {
        series[i].alpha = value;
        if (legendRows) legendRows[i][0].parentNode.style.opacity = value;
    }
    function _setAlpha(i, value) {
        let s = series[i];
        _alpha(i, value);
        if (s.band) {
            // not super robust, will break if two bands are adjacent
            let ip = series[i + 1].band ? i + 1 : i - 1;
            _alpha(ip, value);
        }
    }
    // y-distance
    const distsToCursor = Array(series.length);
    let focused = null;
    function setFocus(i) {
        if (i != focused) {
            //	log("setFocus()", arguments);
            series.forEach((s, i2)=>{
                _setAlpha(i2, i == null || i2 == 0 || i2 == i ? 1 : focus.alpha);
            });
            focused = i;
            paint();
        }
    }
    if (showLegend && cursorFocus) $516155acbd03f669$var$on($516155acbd03f669$var$mouseleave, legendEl, (e)=>{
        if (cursor.locked) return;
        setSeries(null, {
            focus: false
        }, syncOpts.setSeries);
        updateCursor();
    });
    function scaleValueAtPos(pos, scale) {
        let dim = plotWidCss;
        if (scale != xScaleKey) {
            dim = plotHgtCss;
            pos = dim - pos;
        }
        let pct = pos / dim;
        let sc = scales[scale];
        let d = sc.max - sc.min;
        return sc.min + pct * d;
    }
    function closestIdxFromXpos(pos) {
        let v = scaleValueAtPos(pos, xScaleKey);
        return $516155acbd03f669$var$closestIdx(v, data[0], i0, i1);
    }
    self.valToIdx = (val)=>$516155acbd03f669$var$closestIdx(val, data[0]);
    self.posToIdx = closestIdxFromXpos;
    self.posToVal = scaleValueAtPos;
    self.valToPos = (val, scale, can)=>scale == xScaleKey ? $516155acbd03f669$var$getXPos(val, scales[scale], can ? plotWid : plotWidCss, can ? plotLft : 0) : $516155acbd03f669$var$getYPos(val, scales[scale], can ? plotHgt : plotHgtCss, can ? plotTop : 0);
    let inBatch = false;
    let shouldPaint = false;
    let shouldSetScales = false;
    let shouldUpdateCursor = false;
    // defers calling expensive functions
    function batch(fn) {
        inBatch = true;
        fn(self);
        inBatch = false;
        shouldSetScales && setScales();
        shouldUpdateCursor && updateCursor();
        shouldPaint && !didPaint && paint();
        shouldSetScales = shouldUpdateCursor = shouldPaint = didPaint = inBatch;
    }
    self.batch = batch;
    self.setCursor = (opts)=>{
        mouseLeft1 = opts.left;
        mouseTop1 = opts.top;
        //	assign(cursor, opts);
        updateCursor();
    };
    let cursorRaf = 0;
    function updateCursor(ts, src) {
        if (inBatch) {
            shouldUpdateCursor = true;
            return;
        }
        //	ts == null && log("updateCursor()", arguments);
        cursorRaf = 0;
        if (cursor.show) {
            cursor.x && $516155acbd03f669$var$trans(vt, $516155acbd03f669$var$round(mouseLeft1), 0);
            cursor.y && $516155acbd03f669$var$trans(hz, 0, $516155acbd03f669$var$round(mouseTop1));
        }
        let idx;
        // when zooming to an x scale range between datapoints the binary search
        // for nearest min/max indices results in this condition. cheap hack :D
        let noDataInRange = i0 > i1;
        // if cursor hidden, hide points & clear legend vals
        if (mouseLeft1 < 0 || dataLen == 0 || noDataInRange) {
            idx = null;
            for(let i = 0; i < series.length; i++){
                if (i > 0) {
                    distsToCursor[i] = $516155acbd03f669$var$inf;
                    cursorPts.length > 1 && $516155acbd03f669$var$trans(cursorPts[i], -10, -10);
                }
                if (showLegend) {
                    if (i == 0 && multiValLegend) continue;
                    for(let j = 0; j < legendRows[i].length; j++)legendRows[i][j][$516155acbd03f669$var$firstChild].nodeValue = "--";
                }
            }
            if (cursorFocus) setSeries(null, {
                focus: true
            }, syncOpts.setSeries);
        } else {
            //	let pctY = 1 - (y / rect[HEIGHT]);
            idx = closestIdxFromXpos(mouseLeft1);
            let scX = scales[xScaleKey];
            let xPos = $516155acbd03f669$var$round3($516155acbd03f669$var$getXPos(data[0][idx], scX, plotWidCss, 0));
            for(let i2 = 0; i2 < series.length; i2++){
                let s = series[i2];
                if (i2 > 0 && s.show) {
                    let valAtIdx = data[i2][idx];
                    let yPos = valAtIdx == null ? -10 : $516155acbd03f669$var$round3($516155acbd03f669$var$getYPos(valAtIdx, scales[s.scale], plotHgtCss, 0));
                    distsToCursor[i2] = yPos > 0 ? $516155acbd03f669$var$abs(yPos - mouseTop1) : $516155acbd03f669$var$inf;
                    cursorPts.length > 1 && $516155acbd03f669$var$trans(cursorPts[i2], xPos, yPos);
                } else distsToCursor[i2] = $516155acbd03f669$var$inf;
                if (showLegend) {
                    if (idx == cursor.idx || i2 == 0 && multiValLegend) continue;
                    let src1 = i2 == 0 && xScaleDistr == 2 ? data0 : data[i2];
                    let vals = multiValLegend ? s.values(self, i2, idx) : {
                        _: s.value(self, src1[idx], i2, idx)
                    };
                    let j1 = 0;
                    for(let k in vals)legendRows[i2][j1++][$516155acbd03f669$var$firstChild].nodeValue = vals[k];
                }
            }
        }
        // nit: cursor.drag.setSelect is assumed always true
        if (select.show && dragging) {
            let dx = $516155acbd03f669$var$abs(mouseLeft1 - mouseLeft0);
            let dy = $516155acbd03f669$var$abs(mouseTop1 - mouseTop0);
            if (src != null) {
                let [xKey, yKey] = syncOpts.scales;
                // match the dragX/dragY implicitness/explicitness of src
                let sdrag = src.cursor.drag;
                dragX = sdrag._x;
                dragY = sdrag._y;
                if (xKey) {
                    let sc = scales[xKey];
                    let srcLeft = src.posToVal(src.select[$516155acbd03f669$var$LEFT], xKey);
                    let srcRight = src.posToVal(src.select[$516155acbd03f669$var$LEFT] + src.select[$516155acbd03f669$var$WIDTH], xKey);
                    select[$516155acbd03f669$var$LEFT] = $516155acbd03f669$var$getXPos(srcLeft, sc, plotWidCss, 0);
                    select[$516155acbd03f669$var$WIDTH] = $516155acbd03f669$var$abs(select[$516155acbd03f669$var$LEFT] - $516155acbd03f669$var$getXPos(srcRight, sc, plotWidCss, 0));
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$LEFT, select[$516155acbd03f669$var$LEFT]);
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$WIDTH, select[$516155acbd03f669$var$WIDTH]);
                    if (!yKey) {
                        $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$TOP, select[$516155acbd03f669$var$TOP] = 0);
                        $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$HEIGHT, select[$516155acbd03f669$var$HEIGHT] = plotHgtCss);
                    }
                }
                if (yKey) {
                    let sc1 = scales[yKey];
                    let srcTop = src.posToVal(src.select[$516155acbd03f669$var$TOP], yKey);
                    let srcBottom = src.posToVal(src.select[$516155acbd03f669$var$TOP] + src.select[$516155acbd03f669$var$HEIGHT], yKey);
                    select[$516155acbd03f669$var$TOP] = $516155acbd03f669$var$getYPos(srcTop, sc1, plotHgtCss, 0);
                    select[$516155acbd03f669$var$HEIGHT] = $516155acbd03f669$var$abs(select[$516155acbd03f669$var$TOP] - $516155acbd03f669$var$getYPos(srcBottom, sc1, plotHgtCss, 0));
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$TOP, select[$516155acbd03f669$var$TOP]);
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$HEIGHT, select[$516155acbd03f669$var$HEIGHT]);
                    if (!xKey) {
                        $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$LEFT, select[$516155acbd03f669$var$LEFT] = 0);
                        $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$WIDTH, select[$516155acbd03f669$var$WIDTH] = plotWidCss);
                    }
                }
            } else {
                dragX = drag.x && dx >= drag.dist;
                dragY = drag.y && dy >= drag.dist;
                let uni = drag.uni;
                if (uni != null) // only calc drag status if they pass the dist thresh
                {
                    if (dragX && dragY) {
                        dragX = dx >= uni;
                        dragY = dy >= uni;
                        // force unidirectionality when both are under uni limit
                        if (!dragX && !dragY) {
                            if (dy > dx) dragY = true;
                            else dragX = true;
                        }
                    }
                } else if (drag.x && drag.y && (dragX || dragY)) // if omni with no uni then both dragX / dragY should be true if either is true
                dragX = dragY = true;
                if (dragX) {
                    let minX = $516155acbd03f669$var$min(mouseLeft0, mouseLeft1);
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$LEFT, select[$516155acbd03f669$var$LEFT] = minX);
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$WIDTH, select[$516155acbd03f669$var$WIDTH] = dx);
                    if (!dragY) {
                        $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$TOP, select[$516155acbd03f669$var$TOP] = 0);
                        $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$HEIGHT, select[$516155acbd03f669$var$HEIGHT] = plotHgtCss);
                    }
                }
                if (dragY) {
                    let minY = $516155acbd03f669$var$min(mouseTop0, mouseTop1);
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$TOP, select[$516155acbd03f669$var$TOP] = minY);
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$HEIGHT, select[$516155acbd03f669$var$HEIGHT] = dy);
                    if (!dragX) {
                        $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$LEFT, select[$516155acbd03f669$var$LEFT] = 0);
                        $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$WIDTH, select[$516155acbd03f669$var$WIDTH] = plotWidCss);
                    }
                }
                if (!dragX && !dragY) {
                    // the drag didn't pass the dist requirement
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$HEIGHT, select[$516155acbd03f669$var$HEIGHT] = 0);
                    $516155acbd03f669$var$setStylePx(selectDiv, $516155acbd03f669$var$WIDTH, select[$516155acbd03f669$var$WIDTH] = 0);
                }
            }
        }
        cursor.idx = idx;
        cursor.left = mouseLeft1;
        cursor.top = mouseTop1;
        drag._x = dragX;
        drag._y = dragY;
        // if ts is present, means we're implicitly syncing own cursor as a result of debounced rAF
        if (ts != null) {
            // this is not technically a "mousemove" event, since it's debounced, rename to setCursor?
            // since this is internal, we can tweak it later
            sync.pub($516155acbd03f669$var$mousemove, self, mouseLeft1, mouseTop1, plotWidCss, plotHgtCss, idx);
            if (cursorFocus) {
                let minDist = $516155acbd03f669$var$min.apply(null, distsToCursor);
                let fi = null;
                if (minDist <= focus.prox) distsToCursor.some((dist, i)=>{
                    if (dist == minDist) return fi = i;
                });
                setSeries(fi, {
                    focus: true
                }, syncOpts.setSeries);
            }
        }
        ready && fire("setCursor");
    }
    let rect = null;
    function syncRect() {
        rect = over.getBoundingClientRect();
    }
    function mouseMove(e, src, _x, _y, _w, _h, _i) {
        if (cursor.locked) return;
        cacheMouse(e, src, _x, _y, _w, _h, _i, false, e != null);
        if (e != null) {
            if (cursorRaf == 0) cursorRaf = $516155acbd03f669$var$rAF(updateCursor);
        } else updateCursor(null, src);
    }
    function cacheMouse(e, src, _x, _y, _w, _h, _i, initial, snap) {
        if (e != null) {
            _x = e.clientX - rect.left;
            _y = e.clientY - rect.top;
        } else {
            if (_x < 0 || _y < 0) {
                mouseLeft1 = -10;
                mouseTop1 = -10;
                return;
            }
            let [xKey, yKey] = syncOpts.scales;
            if (xKey != null) _x = $516155acbd03f669$var$getXPos(src.posToVal(_x, xKey), scales[xKey], plotWidCss, 0);
            else _x = plotWidCss * (_x / _w);
            if (yKey != null) _y = $516155acbd03f669$var$getYPos(src.posToVal(_y, yKey), scales[yKey], plotHgtCss, 0);
            else _y = plotHgtCss * (_y / _h);
        }
        if (snap) {
            if (_x <= 1 || _x >= plotWidCss - 1) _x = $516155acbd03f669$var$incrRound(_x, plotWidCss);
            if (_y <= 1 || _y >= plotHgtCss - 1) _y = $516155acbd03f669$var$incrRound(_y, plotHgtCss);
        }
        if (initial) {
            mouseLeft0 = _x;
            mouseTop0 = _y;
        } else {
            mouseLeft1 = _x;
            mouseTop1 = _y;
        }
    }
    function hideSelect() {
        setSelect({
            width: 0,
            height: 0
        }, false);
    }
    function mouseDown(e, src, _x, _y, _w, _h, _i) {
        if (src != null || $516155acbd03f669$var$filtMouse(e)) {
            dragging = true;
            dragX = dragY = drag._x = drag._y = false;
            cacheMouse(e, src, _x, _y, _w, _h, _i, true, false);
            if (e != null) {
                $516155acbd03f669$var$on($516155acbd03f669$var$mouseup, $516155acbd03f669$var$doc, mouseUp);
                sync.pub($516155acbd03f669$var$mousedown, self, mouseLeft0, mouseTop0, plotWidCss, plotHgtCss, null);
            }
        }
    }
    function mouseUp(e, src, _x, _y, _w, _h, _i) {
        if (src != null || $516155acbd03f669$var$filtMouse(e)) {
            dragging = drag._x = drag._y = false;
            cacheMouse(e, src, _x, _y, _w, _h, _i, false, true);
            let hasSelect = select[$516155acbd03f669$var$WIDTH] > 0 || select[$516155acbd03f669$var$HEIGHT] > 0;
            hasSelect && setSelect(select);
            if (drag.setScale && hasSelect) {
                //	if (syncKey != null) {
                //		dragX = drag.x;
                //		dragY = drag.y;
                //	}
                batch(()=>{
                    if (dragX) _setScale(xScaleKey, scaleValueAtPos(select[$516155acbd03f669$var$LEFT], xScaleKey), scaleValueAtPos(select[$516155acbd03f669$var$LEFT] + select[$516155acbd03f669$var$WIDTH], xScaleKey));
                    if (dragY) for(let k in scales){
                        let sc = scales[k];
                        if (k != xScaleKey && sc.from == null) _setScale(k, scaleValueAtPos(select[$516155acbd03f669$var$TOP] + select[$516155acbd03f669$var$HEIGHT], k), scaleValueAtPos(select[$516155acbd03f669$var$TOP], k));
                    }
                });
                hideSelect();
            } else if (cursor.lock) {
                cursor.locked = !cursor.locked;
                if (!cursor.locked) updateCursor();
            }
        }
        if (e != null) {
            $516155acbd03f669$var$off($516155acbd03f669$var$mouseup, $516155acbd03f669$var$doc, mouseUp);
            sync.pub($516155acbd03f669$var$mouseup, self, mouseLeft1, mouseTop1, plotWidCss, plotHgtCss, null);
        }
    }
    function mouseLeave(e, src, _x, _y, _w, _h, _i) {
        if (!cursor.locked) {
            let _dragging = dragging;
            if (dragging) {
                // handle case when mousemove aren't fired all the way to edges by browser
                let snapX = true;
                let snapY = true;
                let snapProx = 10;
                if (dragX && dragY) {
                    // maybe omni corner snap
                    snapX = mouseLeft1 <= snapProx || mouseLeft1 >= plotWidCss - snapProx;
                    snapY = mouseTop1 <= snapProx || mouseTop1 >= plotHgtCss - snapProx;
                }
                if (dragX && snapX) {
                    let dLft = mouseLeft1;
                    let dRgt = plotWidCss - mouseLeft1;
                    let xMin = $516155acbd03f669$var$min(dLft, dRgt);
                    if (xMin == dLft) mouseLeft1 = 0;
                    if (xMin == dRgt) mouseLeft1 = plotWidCss;
                }
                if (dragY && snapY) {
                    let dTop = mouseTop1;
                    let dBtm = plotHgtCss - mouseTop1;
                    let yMin = $516155acbd03f669$var$min(dTop, dBtm);
                    if (yMin == dTop) mouseTop1 = 0;
                    if (yMin == dBtm) mouseTop1 = plotHgtCss;
                }
                updateCursor(1);
                dragging = false;
            }
            mouseLeft1 = -10;
            mouseTop1 = -10;
            // passing a non-null timestamp to force sync/mousemove event
            updateCursor(1);
            if (_dragging) dragging = _dragging;
        }
    }
    function dblClick(e, src, _x, _y, _w, _h, _i) {
        autoScaleX();
        hideSelect();
        if (e != null) sync.pub($516155acbd03f669$var$dblclick, self, mouseLeft1, mouseTop1, plotWidCss, plotHgtCss, null);
    }
    // internal pub/sub
    const events = {};
    events[$516155acbd03f669$var$mousedown] = mouseDown;
    events[$516155acbd03f669$var$mousemove] = mouseMove;
    events[$516155acbd03f669$var$mouseup] = mouseUp;
    events[$516155acbd03f669$var$dblclick] = dblClick;
    events["setSeries"] = (e, src, idx, opts)=>{
        setSeries(idx, opts);
    };
    let deb;
    if (cursor.show) {
        $516155acbd03f669$var$on($516155acbd03f669$var$mousedown, over, mouseDown);
        $516155acbd03f669$var$on($516155acbd03f669$var$mousemove, over, mouseMove);
        $516155acbd03f669$var$on($516155acbd03f669$var$mouseenter, over, syncRect);
        // this has to be rAF'd so it always fires after the last queued/rAF'd updateCursor
        $516155acbd03f669$var$on($516155acbd03f669$var$mouseleave, over, (e)=>{
            $516155acbd03f669$var$rAF(mouseLeave);
        });
        $516155acbd03f669$var$on($516155acbd03f669$var$dblclick, over, dblClick);
        deb = $516155acbd03f669$var$debounce(syncRect, 100);
        $516155acbd03f669$var$on($516155acbd03f669$var$resize, $516155acbd03f669$var$win, deb);
        $516155acbd03f669$var$on($516155acbd03f669$var$scroll, $516155acbd03f669$var$win, deb);
        self.syncRect = syncRect;
    }
    // external on/off
    const hooks = self.hooks = opts.hooks || {};
    function fire(evName, a1, a2) {
        if (evName in hooks) hooks[evName].forEach((fn)=>{
            fn.call(null, self, a1, a2);
        });
    }
    (opts.plugins || []).forEach((p)=>{
        for(let evName in p.hooks)hooks[evName] = (hooks[evName] || []).concat(p.hooks[evName]);
    });
    const syncOpts = $516155acbd03f669$var$assign({
        key: null,
        setSeries: false,
        scales: [
            xScaleKey,
            null
        ]
    }, cursor.sync);
    const syncKey = syncOpts.key;
    const sync = syncKey != null ? $516155acbd03f669$var$syncs[syncKey] = $516155acbd03f669$var$syncs[syncKey] || $516155acbd03f669$var$_sync() : $516155acbd03f669$var$_sync();
    sync.sub(self);
    function pub(type, src, x, y, w, h, i) {
        events[type](null, src, x, y, w, h, i);
    }
    self.pub = pub;
    function destroy() {
        sync.unsub(self);
        $516155acbd03f669$var$off($516155acbd03f669$var$resize, $516155acbd03f669$var$win, deb);
        $516155acbd03f669$var$off($516155acbd03f669$var$scroll, $516155acbd03f669$var$win, deb);
        root.remove();
        fire("destroy");
    }
    self.destroy = destroy;
    function _init() {
        _setSize(opts[$516155acbd03f669$var$WIDTH], opts[$516155acbd03f669$var$HEIGHT]);
        fire("init", opts, data);
        setData(data || opts.data, false);
        if (pendScales[xScaleKey]) setScale(xScaleKey, pendScales[xScaleKey]);
        else autoScaleX();
        setSelect(select, false);
        ready = true;
        fire("ready");
    }
    if (then) {
        if (then instanceof HTMLElement) {
            then.appendChild(root);
            _init();
        } else then(self, _init);
    } else _init();
    return self;
}
$516155acbd03f669$var$uPlot.assign = $516155acbd03f669$var$assign;
$516155acbd03f669$var$uPlot.rangeNum = $516155acbd03f669$var$rangeNum;
$516155acbd03f669$var$uPlot.fmtDate = $516155acbd03f669$var$fmtDate;
$516155acbd03f669$var$uPlot.tzDate = $516155acbd03f669$var$tzDate;
var $516155acbd03f669$export$2e2bcd8739ae039 = $516155acbd03f669$var$uPlot;


class $16ad6ced048dbe5c$export$4ab14d3905033d17 {
    static scale(data, tickCount, maxFactor) {
        const { min: min , max: max  } = $16ad6ced048dbe5c$export$4ab14d3905033d17.calculateBounds(data);
        let factor = 1;
        while(true){
            const scale = Math.pow(10, factor);
            const scaledMin = min - min % scale;
            let scaledMax = max + (max % scale === 0 ? 0 : scale - max % scale);
            // Prevent min/max from being equal (and generating 0 ticks)
            // This happens when all data points are products of scale value
            if (scaledMin === scaledMax) scaledMax += scale;
            const ticks = (scaledMax - scaledMin) / scale;
            if (ticks <= tickCount || typeof maxFactor === "number" && factor === maxFactor) return {
                scaledMin: scaledMin,
                scaledMax: scaledMax,
                scale: scale
            };
            else // Too many steps between min/max, increase factor and try again
            factor++;
        }
    }
    static scaleMatrix(data, tickCount, maxFactor) {
        const nonNullData = data.flat().filter((val)=>val !== null);
        // when used with the spread operator large nonNullData/data arrays can reach the max call stack size
        // use reduce calls to safely determine min/max values for any size of array
        // https://stackoverflow.com/questions/63705432/maximum-call-stack-size-exceeded-when-using-the-dots-operator/63706516#63706516
        const max = nonNullData.reduce((a, b)=>{
            return Math.max(a, b);
        }, Number.NEGATIVE_INFINITY);
        return $16ad6ced048dbe5c$export$4ab14d3905033d17.scale([
            0,
            $16ad6ced048dbe5c$export$4ab14d3905033d17.isFiniteOrZero(max)
        ], tickCount, maxFactor);
    }
    static generateTicks(min, max, step) {
        const ticks = [];
        for(let i = min; i <= max; i += step)ticks.push(i);
        return ticks;
    }
    static calculateBounds(data) {
        if (data.length === 0) return {
            min: 0,
            max: 0
        };
        else {
            const nonNullData = data.filter((val)=>val !== null);
            // when used with the spread operator large nonNullData/data arrays can reach the max call stack size
            // use reduce calls to safely determine min/max values for any size of array
            // https://stackoverflow.com/questions/63705432/maximum-call-stack-size-exceeded-when-using-the-dots-operator/63706516#63706516
            const min = nonNullData.reduce((a, b)=>{
                return Math.min(a, b);
            }, Number.POSITIVE_INFINITY);
            const max = nonNullData.reduce((a, b)=>{
                return Math.max(a, b);
            }, Number.NEGATIVE_INFINITY);
            return {
                min: $16ad6ced048dbe5c$export$4ab14d3905033d17.isFiniteOrZero(min),
                max: $16ad6ced048dbe5c$export$4ab14d3905033d17.isFiniteOrZero(max)
            };
        }
    }
    static isFiniteOrZero(val) {
        return Number.isFinite(val) ? val : 0;
    }
}


function $084fcb6f6aaea09e$var$_defineProperty(obj, key, value) {
    if (key in obj) Object.defineProperty(obj, key, {
        value: value,
        enumerable: true,
        configurable: true,
        writable: true
    });
    else obj[key] = value;
    return obj;
}
class $084fcb6f6aaea09e$export$28c660c63b792dea {
    constructor(){
        $084fcb6f6aaea09e$var$_defineProperty(this, "hide", ()=>{
            this._div.style.display = "none";
        });
        this._div = document.getElementById("tooltip");
    }
    set(x, y, offsetX, offsetY, html) {
        this._div.innerHTML = html;
        // Assign display: block so that the offsetWidth is valid
        this._div.style.display = "block";
        // Prevent the div from overflowing the page width
        const tooltipWidth = this._div.offsetWidth;
        // 1.2 is a magic number used to pad the offset to ensure the tooltip
        // never gets close or surpasses the page's X width
        if (x + offsetX + tooltipWidth * 1.2 > window.innerWidth) {
            x -= tooltipWidth;
            offsetX *= -1;
        }
        this._div.style.top = `${y + offsetY}px`;
        this._div.style.left = `${x + offsetX}px`;
    }
}
class $084fcb6f6aaea09e$export$32fbfacc5d962e0c {
    constructor(){
        this._div = document.getElementById("status-text");
    }
    set(text) {
        this._div.innerText = text;
        this._div.style.display = "block";
    }
    hide() {
        this._div.style.display = "none";
    }
}
// Minecraft Java Edition default server port: 25565
// Minecraft Bedrock Edition default server port: 19132
const $084fcb6f6aaea09e$var$MINECRAFT_DEFAULT_PORTS = [
    25565,
    19132
];
function $084fcb6f6aaea09e$export$b9d29ed6ecff7b9d(ip, port) {
    if (port && !$084fcb6f6aaea09e$var$MINECRAFT_DEFAULT_PORTS.includes(port)) return `${ip}:${port}`;
    return ip;
}
function $084fcb6f6aaea09e$export$aadb186058d814a7(versions, knownVersions) {
    if (!versions || !versions.length || !knownVersions || !knownVersions.length) return;
    let currentVersionGroup = [];
    const versionGroups = [];
    for(let i = 0; i < versions.length; i++){
        // Look for value mismatch between the previous index
        // Require i > 0 since lastVersionIndex is undefined for i === 0
        if (i > 0 && versions[i] - versions[i - 1] !== 1) {
            versionGroups.push(currentVersionGroup);
            currentVersionGroup = [];
        }
        currentVersionGroup.push(versions[i]);
    }
    // Ensure the last versionGroup is always pushed
    if (currentVersionGroup.length > 0) versionGroups.push(currentVersionGroup);
    if (versionGroups.length === 0) return;
    // Remap individual versionGroups values into named versions
    return versionGroups.map((versionGroup)=>{
        const startVersion = knownVersions[versionGroup[0]];
        if (versionGroup.length === 1) // A versionGroup may contain a single version, only return its name
        // This is a cosmetic catch to avoid version labels like 1.0-1.0
        return startVersion;
        else {
            const endVersion = knownVersions[versionGroup[versionGroup.length - 1]];
            return `${startVersion}-${endVersion}`;
        }
    }).join(", ");
}
function $084fcb6f6aaea09e$export$b144b3a88a3e7934(secs) {
    const date = new Date(0);
    date.setUTCSeconds(secs);
    return date.toLocaleTimeString(); // 예: "오후 3:15:00"
}
function $084fcb6f6aaea09e$export$3ae94a2503e890a1(secs) {
    const date = new Date(0);
    date.setUTCSeconds(secs);
    return date.toLocaleDateString();
}
function $084fcb6f6aaea09e$export$32e6b1ae352e94d6(x, over) {
    const val = Math.round(x / over * 1000) / 10;
    return `${val}%`;
}
function $084fcb6f6aaea09e$export$f5dd818bff069720(x) {
    if (typeof x !== "number") return "-";
    else return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}


function $9b3527dabeeb30eb$export$2f2e390e2470b67e(onHover) {
    let element;
    return {
        hooks: {
            init: (u)=>{
                element = u.root.querySelector(".over");
                element.onmouseenter = ()=>onHover();
                element.onmouseleave = ()=>onHover();
            },
            setCursor: (u)=>{
                const { left: left , top: top , idx: idx  } = u.cursor;
                if (idx === null) onHover();
                else {
                    const bounds = element.getBoundingClientRect();
                    onHover({
                        left: bounds.left + left + window.pageXOffset,
                        top: bounds.top + top + window.pageYOffset
                    }, idx);
                }
            }
        }
    };
}


var $e6cefa8b6ed631c3$exports = {};

$e6cefa8b6ed631c3$exports = (parcelRequire("h8nox")).getBundleURL("6aCO0") + "missing_favicon.22b4e869.svg";


function $72f844c467870b3a$var$_defineProperty(obj, key, value) {
    if (key in obj) Object.defineProperty(obj, key, {
        value: value,
        enumerable: true,
        configurable: true,
        writable: true
    });
    else obj[key] = value;
    return obj;
}
class $72f844c467870b3a$export$1115b10cc9bb0fa8 {
    constructor(app){
        $72f844c467870b3a$var$_defineProperty(this, "getServerRegistrations", ()=>Object.values(this._registeredServers));
        this._app = app;
        this._serverIdsByName = [];
        this._serverDataById = [];
        this._registeredServers = [];
    }
    assignServers(servers) {
        for(let i = 0; i < servers.length; i++){
            const data = servers[i];
            this._serverIdsByName[data.name] = i;
            this._serverDataById[i] = data;
        }
    }
    createServerRegistration(serverId) {
        const serverData = this._serverDataById[serverId];
        const serverRegistration = new $72f844c467870b3a$export$c6f081893021db7f(this._app, serverId, serverData);
        this._registeredServers[serverId] = serverRegistration;
        return serverRegistration;
    }
    getServerRegistration(serverKey) {
        if (typeof serverKey === "string") {
            const serverId = this._serverIdsByName[serverKey];
            return this._registeredServers[serverId];
        } else if (typeof serverKey === "number") return this._registeredServers[serverKey];
    }
    reset() {
        this._serverIdsByName = [];
        this._serverDataById = [];
        this._registeredServers = [];
        // Reset modified DOM structures
        document.getElementById("server-list").innerHTML = "";
    }
}
class $72f844c467870b3a$export$c6f081893021db7f {
    constructor(app, serverId, data){
        $72f844c467870b3a$var$_defineProperty(this, "playerCount", 0);
        $72f844c467870b3a$var$_defineProperty(this, "isVisible", true);
        $72f844c467870b3a$var$_defineProperty(this, "isFavorite", false);
        $72f844c467870b3a$var$_defineProperty(this, "rankIndex", void 0);
        $72f844c467870b3a$var$_defineProperty(this, "lastRecordData", void 0);
        $72f844c467870b3a$var$_defineProperty(this, "lastPeakData", void 0);
        this._app = app;
        this.serverId = serverId;
        this.data = data;
        this._graphData = [
            [],
            []
        ];
        this._failedSequentialPings = 0;
    }
    getGraphDataIndex() {
        return this.serverId + 1;
    }
    addGraphPoints(points, timestampPoints) {
        this._graphData = [
            timestampPoints.slice(),
            points
        ];
    }
    buildPlotInstance() {
        const tickCount = 4;
        // eslint-disable-next-line new-cap
        this._plotInstance = new (0, $516155acbd03f669$export$2e2bcd8739ae039)({
            plugins: [
                (0, $9b3527dabeeb30eb$export$2f2e390e2470b67e)((pos, id)=>{
                    if (pos) {
                        const playerCount = this._graphData[1][id];
                        if (typeof playerCount !== "number") this._app.tooltip.hide();
                        else this._app.tooltip.set(pos.left, pos.top, 10, 10, `${(0, $084fcb6f6aaea09e$export$f5dd818bff069720)(playerCount)}명<br>${(0, $084fcb6f6aaea09e$export$b144b3a88a3e7934)(this._graphData[0][id])}`);
                    } else this._app.tooltip.hide();
                })
            ],
            height: 100,
            width: 400,
            cursor: {
                y: false,
                drag: {
                    setScale: false,
                    x: false,
                    y: false
                },
                sync: {
                    key: "minetrack-server",
                    setSeries: true
                }
            },
            series: [
                {},
                {
                    stroke: "#E9E581",
                    width: 2,
                    value: (_, raw)=>`${(0, $084fcb6f6aaea09e$export$f5dd818bff069720)(raw)}명`,
                    spanGaps: true,
                    points: {
                        show: false
                    }
                }
            ],
            axes: [
                {
                    show: false
                },
                {
                    ticks: {
                        show: false
                    },
                    font: '14px "Open Sans", sans-serif',
                    stroke: "#A3A3A3",
                    size: 55,
                    grid: {
                        stroke: "#333",
                        width: 1
                    },
                    split: ()=>{
                        const { scaledMin: scaledMin , scaledMax: scaledMax , scale: scale  } = (0, $16ad6ced048dbe5c$export$4ab14d3905033d17).scale(this._graphData[1], tickCount);
                        const ticks = (0, $16ad6ced048dbe5c$export$4ab14d3905033d17).generateTicks(scaledMin, scaledMax, scale);
                        return ticks;
                    }
                }
            ],
            scales: {
                y: {
                    auto: false,
                    range: ()=>{
                        const { scaledMin: scaledMin , scaledMax: scaledMax  } = (0, $16ad6ced048dbe5c$export$4ab14d3905033d17).scale(this._graphData[1], tickCount);
                        return [
                            scaledMin,
                            scaledMax
                        ];
                    }
                }
            },
            legend: {
                show: false
            }
        }, this._graphData, document.getElementById(`chart_${this.serverId}`));
    }
    handlePing(payload, timestamp) {
        if (typeof payload.playerCount === "number") {
            this.playerCount = payload.playerCount;
            // Reset failed ping counter to ensure the next connection error
            // doesn't instantly retrigger a layout change
            this._failedSequentialPings = 0;
        } else // Attempt to retain a copy of the cached playerCount for up to N failed pings
        // This prevents minor connection issues from constantly reshuffling the layout
        if (++this._failedSequentialPings > 5) this.playerCount = 0;
        // Use payload.playerCount so nulls WILL be pushed into the graphing data
        this._graphData[0].push(timestamp);
        this._graphData[1].push(payload.playerCount);
        // Trim graphData to within the max length by shifting out the leading elements
        for (const series of this._graphData)if (series.length > this._app.publicConfig.serverGraphMaxLength) series.shift();
        // Redraw the plot instance
        this._plotInstance.setData(this._graphData);
    }
    updateServerRankIndex(rankIndex) {
        this.rankIndex = rankIndex;
        document.getElementById(`ranking_${this.serverId}`).innerText = `#${rankIndex + 1}`;
    }
    _renderValue(prefix, handler) {
        const labelElement = document.getElementById(`${prefix}_${this.serverId}`);
        labelElement.style.display = "block";
        const valueElement = document.getElementById(`${prefix}-value_${this.serverId}`);
        const targetElement = valueElement || labelElement;
        if (targetElement) {
            if (typeof handler === "function") handler(targetElement);
            else targetElement.innerText = handler;
        }
    }
    _hideValue(prefix) {
        const element = document.getElementById(`${prefix}_${this.serverId}`);
        element.style.display = "none";
    }
    updateServerStatus(ping, minecraftVersions) {
        if (ping.versions) this._renderValue("version", (0, $084fcb6f6aaea09e$export$aadb186058d814a7)(ping.versions, minecraftVersions[this.data.type]) || "");
        if (ping.recordData) {
            this._renderValue("record", (element)=>{
                if (ping.recordData.timestamp > 0) {
                    element.innerText = `${(0, $084fcb6f6aaea09e$export$f5dd818bff069720)(ping.recordData.playerCount)} (${(0, $084fcb6f6aaea09e$export$3ae94a2503e890a1)(ping.recordData.timestamp)})`;
                    element.title = `At ${(0, $084fcb6f6aaea09e$export$3ae94a2503e890a1)(ping.recordData.timestamp)} ${(0, $084fcb6f6aaea09e$export$b144b3a88a3e7934)(ping.recordData.timestamp)}`;
                } else element.innerText = (0, $084fcb6f6aaea09e$export$f5dd818bff069720)(ping.recordData.playerCount);
            });
            this.lastRecordData = ping.recordData;
        }
        if (ping.graphPeakData) {
            this._renderValue("peak", (element)=>{
                element.innerText = (0, $084fcb6f6aaea09e$export$f5dd818bff069720)(ping.graphPeakData.playerCount);
                element.title = `${(0, $084fcb6f6aaea09e$export$b144b3a88a3e7934)(ping.graphPeakData.timestamp)}에 달성`;
            });
            this.lastPeakData = ping.graphPeakData;
        }
        if (ping.error) {
            this._hideValue("player-count");
            this._renderValue("error", ping.error.message);
        } else if (typeof ping.playerCount !== "number") {
            this._hideValue("player-count");
            // If the frontend has freshly connection, and the server's last ping was in error, it may not contain an error object
            // In this case playerCount will safely be null, so provide a generic error message instead
            this._renderValue("error", "Failed to ping");
        } else if (typeof ping.playerCount === "number") {
            this._hideValue("error");
            this._renderValue("player-count", (0, $084fcb6f6aaea09e$export$f5dd818bff069720)(ping.playerCount));
        }
        // An updated favicon has been sent, update the src
        if (ping.favicon) {
            const faviconElement = document.getElementById(`favicon_${this.serverId}`);
            // Since favicons may be URLs, only update the attribute when it has changed
            // Otherwise the browser may send multiple requests to the same URL
            if (faviconElement.getAttribute("src") !== ping.favicon) faviconElement.setAttribute("src", ping.favicon);
        }
    }
    initServerStatus(latestPing) {
        const serverElement = document.createElement("div");
        serverElement.id = `container_${this.serverId}`;
        serverElement.innerHTML = `<div class="column column-favicon">
        <img class="server-favicon" src="${latestPing.favicon || (0, (/*@__PURE__*/$parcel$interopDefault($e6cefa8b6ed631c3$exports)))}" id="favicon_${this.serverId}" title="${this.data.name}\n${(0, $084fcb6f6aaea09e$export$b9d29ed6ecff7b9d)(this.data.ip, this.data.port)}">
        <span class="server-rank" id="ranking_${this.serverId}"></span>
      </div>
      <div class="column column-status">
        <h3 class="server-name"><span class="${this._app.favoritesManager.getIconClass(this.isFavorite)}" id="favorite-toggle_${this.serverId}"></span> ${this.data.name}</h3>
        <span class="server-error" id="error_${this.serverId}"></span>
        <span class="server-label" id="player-count_${this.serverId}">접속자 수: <span class="server-value" id="player-count-value_${this.serverId}"></span></span>
        <span class="server-label" id="peak_${this.serverId}">${this._app.publicConfig.graphDurationLabel}간 최고 접속자: <span class="server-value" id="peak-value_${this.serverId}">-</span></span>
        <span class="server-label" id="record_${this.serverId}">최고 기록: <span class="server-value" id="record-value_${this.serverId}">-</span></span>
        <span class="server-label" id="version_${this.serverId}"></span>
      </div>
      <div class="column column-graph" id="chart_${this.serverId}"></div>`;
        serverElement.setAttribute("class", "server");
        document.getElementById("server-list").appendChild(serverElement);
    }
    updateHighlightedValue(selectedCategory) {
        [
            "player-count",
            "peak",
            "record"
        ].forEach((category)=>{
            const labelElement = document.getElementById(`${category}_${this.serverId}`);
            const valueElement = document.getElementById(`${category}-value_${this.serverId}`);
            if (selectedCategory && category === selectedCategory) {
                labelElement.setAttribute("class", "server-highlighted-label");
                valueElement.setAttribute("class", "server-highlighted-value");
            } else {
                labelElement.setAttribute("class", "server-label");
                valueElement.setAttribute("class", "server-value");
            }
        });
    }
    initEventListeners() {
        document.getElementById(`favorite-toggle_${this.serverId}`).addEventListener("click", ()=>{
            this._app.favoritesManager.handleFavoriteButtonClick(this);
        }, false);
    }
}


class $374c2a3ea73d3d01$export$c91428cbd4f5850d {
    constructor(app){
        this._app = app;
        this._hasRequestedHistoryGraph = false;
        this._reconnectDelayBase = 0;
    }
    reset() {
        this._hasRequestedHistoryGraph = false;
    }
    createWebSocket() {
        let webSocketProtocol = "ws:";
        if (location.protocol === "https:") webSocketProtocol = "wss:";
        this._webSocket = new WebSocket(`${webSocketProtocol}//${location.host}`);
        // The backend will automatically push data once connected
        this._webSocket.onopen = ()=>{
            this._app.caption.set("Loading...");
            // Reset reconnection scheduling since the WebSocket has been established
            this._reconnectDelayBase = 0;
        };
        this._webSocket.onclose = (event)=>{
            this._app.handleDisconnect();
            // Modify page state to display loading overlay
            // Code 1006 denotes "Abnormal closure", most likely from the server or client losing connection
            // See https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent
            // Treat other codes as active errors (besides connectivity errors) when displaying the message
            if (event.code === 1006) this._app.caption.set("Lost connection!");
            else this._app.caption.set("Disconnected due to error.");
            // Schedule socket reconnection attempt
            this.scheduleReconnect();
        };
        this._webSocket.onmessage = (message)=>{
            const payload = JSON.parse(message.data);
            switch(payload.message){
                case "init":
                    this._app.setPublicConfig(payload.config);
                    // Display the main page component
                    // Called here instead of syncComplete so the DOM can be drawn prior to the graphs being drawn
                    this._app.setPageReady(true);
                    // Allow the graphDisplayManager to control whether or not the historical graph is loaded
                    // Defer to isGraphVisible from the publicConfig to understand if the frontend will ever receive a graph payload
                    if (this._app.publicConfig.isGraphVisible) this.sendHistoryGraphRequest();
                    payload.servers.forEach((serverPayload, serverId)=>{
                        this._app.addServer(serverId, serverPayload, payload.timestampPoints);
                    });
                    // Init payload contains all data needed to render the page
                    // Alert the app it is ready
                    this._app.handleSyncComplete();
                    break;
                case "updateServers":
                    for(let serverId = 0; serverId < payload.updates.length; serverId++){
                        // The backend may send "update" events prior to receiving all "add" events
                        // A server has only been added once it's ServerRegistration is defined
                        // Checking undefined protects from this race condition
                        const serverRegistration = this._app.serverRegistry.getServerRegistration(serverId);
                        const serverUpdate = payload.updates[serverId];
                        if (serverRegistration) {
                            serverRegistration.handlePing(serverUpdate, payload.timestamp);
                            serverRegistration.updateServerStatus(serverUpdate, this._app.publicConfig.minecraftVersions);
                        }
                    }
                    // Bulk add playerCounts into graph during #updateHistoryGraph
                    if (payload.updateHistoryGraph) {
                        this._app.graphDisplayManager.addGraphPoint(payload.timestamp, Object.values(payload.updates).map((update)=>update.playerCount));
                        // Run redraw tasks after handling bulk updates
                        this._app.graphDisplayManager.redraw();
                    }
                    this._app.percentageBar.redraw();
                    this._app.updateGlobalStats();
                    break;
                case "historyGraph":
                    {
                        this._app.graphDisplayManager.buildPlotInstance(payload.timestamps, payload.graphData);
                        // Build checkbox elements for graph controls
                        let lastRowCounter = 0;
                        let controlsHTML = "";
                        this._app.serverRegistry.getServerRegistrations().map((serverRegistration)=>serverRegistration.data.name).sort().forEach((serverName)=>{
                            const serverRegistration = this._app.serverRegistry.getServerRegistration(serverName);
                            controlsHTML += `<td><label>
                <input type="checkbox" class="graph-control" minetrack-server-id="${serverRegistration.serverId}" ${serverRegistration.isVisible ? "checked" : ""}>
                ${serverName}
                </label></td>`;
                            // Occasionally break table rows using a magic number
                            if (++lastRowCounter % 6 === 0) controlsHTML += "</tr><tr>";
                        });
                        // Apply generated HTML and show controls
                        document.getElementById("big-graph-checkboxes").innerHTML = `<table><tr>${controlsHTML}</tr></table>`;
                        document.getElementById("big-graph-controls").style.display = "block";
                        // Bind click event for updating graph data
                        this._app.graphDisplayManager.initEventListeners();
                        break;
                    }
            }
        };
    }
    scheduleReconnect() {
        // Release any active WebSocket references
        this._webSocket = undefined;
        this._reconnectDelayBase++;
        // Exponential backoff for reconnection attempts
        // Clamp ceiling value to 30 seconds
        this._reconnectDelaySeconds = Math.min(this._reconnectDelayBase * this._reconnectDelayBase, 30);
        const reconnectInterval = setInterval(()=>{
            this._reconnectDelaySeconds--;
            if (this._reconnectDelaySeconds === 0) {
                // Explicitly clear interval, this avoids race conditions
                // #clearInterval first to avoid potential errors causing pre-mature returns
                clearInterval(reconnectInterval);
                // Update displayed text
                this._app.caption.set("Reconnecting...");
                // Attempt reconnection
                // Only attempt when reconnectDelaySeconds === 0 and not <= 0, otherwise multiple attempts may be started
                this.createWebSocket();
            } else if (this._reconnectDelaySeconds > 0) // Update displayed text
            this._app.caption.set(`Reconnecting in ${this._reconnectDelaySeconds}s...`);
        }, 1000);
    }
    sendHistoryGraphRequest() {
        if (!this._hasRequestedHistoryGraph) {
            this._hasRequestedHistoryGraph = true;
            // Send request as a plain text string to avoid the server needing to parse JSON
            // This is mostly to simplify the backend server's need for error handling
            this._webSocket.send("requestHistoryGraph");
        }
    }
}


function $4a2c720cc391868f$var$_defineProperty(obj, key, value) {
    if (key in obj) Object.defineProperty(obj, key, {
        value: value,
        enumerable: true,
        configurable: true,
        writable: true
    });
    else obj[key] = value;
    return obj;
}
const $4a2c720cc391868f$var$SORT_OPTIONS = [
    {
        getName: ()=>"접속자 수",
        sortFunc: (a, b)=>b.playerCount - a.playerCount,
        highlightedValue: "player-count"
    },
    {
        getName: (app)=>{
            return `${app.publicConfig.graphDurationLabel} 최고접속`;
        },
        sortFunc: (a, b)=>{
            if (!a.lastPeakData && !b.lastPeakData) return 0;
            else if (a.lastPeakData && !b.lastPeakData) return -1;
            else if (b.lastPeakData && !a.lastPeakData) return 1;
            return b.lastPeakData.playerCount - a.lastPeakData.playerCount;
        },
        testFunc: (app)=>{
            // Require at least one ServerRegistration to have a lastPeakData value defined
            for (const serverRegistration of app.serverRegistry.getServerRegistrations()){
                if (serverRegistration.lastPeakData) return true;
            }
            return false;
        },
        highlightedValue: "peak"
    },
    {
        getName: ()=>"최고 기록",
        sortFunc: (a, b)=>{
            if (!a.lastRecordData && !b.lastRecordData) return 0;
            else if (a.lastRecordData && !b.lastRecordData) return -1;
            else if (b.lastRecordData && !a.lastRecordData) return 1;
            return b.lastRecordData.playerCount - a.lastRecordData.playerCount;
        },
        testFunc: (app)=>{
            // Require at least one ServerRegistration to have a lastRecordData value defined
            for (const serverRegistration of app.serverRegistry.getServerRegistrations()){
                if (serverRegistration.lastRecordData) return true;
            }
            return false;
        },
        highlightedValue: "record"
    }
];
const $4a2c720cc391868f$var$SORT_OPTION_INDEX_DEFAULT = 0;
const $4a2c720cc391868f$var$SORT_OPTION_INDEX_STORAGE_KEY = "minetrack_sort_option_index";
class $4a2c720cc391868f$export$765594e5be71a851 {
    constructor(app){
        $4a2c720cc391868f$var$_defineProperty(this, "handleSortByButtonClick", ()=>{
            while(true){
                // Increment to the next sort option, wrap around if needed
                this._sortOptionIndex = (this._sortOptionIndex + 1) % $4a2c720cc391868f$var$SORT_OPTIONS.length;
                // Only break if the sortOption is supported
                // This can technically cause an infinite loop, but never should assuming
                // at least one sortOption does not implement the test OR always returns true
                const sortOption = $4a2c720cc391868f$var$SORT_OPTIONS[this._sortOptionIndex];
                if (!sortOption.testFunc || sortOption.testFunc(this._app)) break;
            }
            // Redraw the button and sort the servers
            this.updateSortOption();
            // Save the updated option selection
            this.updateLocalStorage();
        });
        $4a2c720cc391868f$var$_defineProperty(this, "updateSortOption", ()=>{
            const sortOption = $4a2c720cc391868f$var$SORT_OPTIONS[this._sortOptionIndex];
            // Pass app instance so sortOption names can be dynamically generated
            this._textElement.innerText = sortOption.getName(this._app);
            // Update all servers highlighted values
            for (const serverRegistration of this._app.serverRegistry.getServerRegistrations())serverRegistration.updateHighlightedValue(sortOption.highlightedValue);
            this.sortServers();
        });
        $4a2c720cc391868f$var$_defineProperty(this, "sortServers", ()=>{
            const sortOption = $4a2c720cc391868f$var$SORT_OPTIONS[this._sortOptionIndex];
            const sortedServers = this._app.serverRegistry.getServerRegistrations().sort((a, b)=>{
                if (a.isFavorite && !b.isFavorite) return -1;
                else if (b.isFavorite && !a.isFavorite) return 1;
                return sortOption.sortFunc(a, b);
            });
            // Test if sortedServers has changed from the previous listing
            // This avoids DOM updates and graphs being redrawn
            const sortedServerIds = sortedServers.map((server)=>server.serverId);
            if (this._lastSortedServers) {
                let allMatch = true;
                // Test if the arrays have actually changed
                // No need to length check, they are the same source data each time
                for(let i = 0; i < sortedServerIds.length; i++)if (sortedServerIds[i] !== this._lastSortedServers[i]) {
                    allMatch = false;
                    break;
                }
                if (allMatch) return;
            }
            this._lastSortedServers = sortedServerIds;
            // Sort a ServerRegistration list by the sortOption ONLY
            // This is used to determine the ServerRegistration's rankIndex without #isFavorite skewing values
            const rankIndexSort = this._app.serverRegistry.getServerRegistrations().sort(sortOption.sortFunc);
            // Update the DOM structure
            sortedServers.forEach(function(serverRegistration) {
                const parentElement = document.getElementById("server-list");
                const serverElement = document.getElementById(`container_${serverRegistration.serverId}`);
                parentElement.appendChild(serverElement);
                // Set the ServerRegistration's rankIndex to its indexOf the normal sort
                serverRegistration.updateServerRankIndex(rankIndexSort.indexOf(serverRegistration));
            });
        });
        this._app = app;
        this._buttonElement = document.getElementById("sort-by");
        this._textElement = document.getElementById("sort-by-text");
        this._sortOptionIndex = $4a2c720cc391868f$var$SORT_OPTION_INDEX_DEFAULT;
    }
    reset() {
        this._lastSortedServers = undefined;
        // Reset modified DOM structures
        this._buttonElement.style.display = "none";
        this._textElement.innerText = "...";
        // Remove bound DOM event listeners
        this._buttonElement.removeEventListener("click", this.handleSortByButtonClick);
    }
    loadLocalStorage() {
        if (typeof localStorage !== "undefined") {
            const sortOptionIndex = localStorage.getItem($4a2c720cc391868f$var$SORT_OPTION_INDEX_STORAGE_KEY);
            if (sortOptionIndex) this._sortOptionIndex = parseInt(sortOptionIndex);
        }
    }
    updateLocalStorage() {
        if (typeof localStorage !== "undefined") {
            if (this._sortOptionIndex !== $4a2c720cc391868f$var$SORT_OPTION_INDEX_DEFAULT) localStorage.setItem($4a2c720cc391868f$var$SORT_OPTION_INDEX_STORAGE_KEY, this._sortOptionIndex);
            else localStorage.removeItem($4a2c720cc391868f$var$SORT_OPTION_INDEX_STORAGE_KEY);
        }
    }
    show() {
        // Load the saved option selection, if any
        this.loadLocalStorage();
        this.updateSortOption();
        // Bind DOM event listeners
        // This is removed by #reset to avoid multiple listeners
        this._buttonElement.addEventListener("click", this.handleSortByButtonClick);
        // Show #sort-by element
        this._buttonElement.style.display = "inline-block";
    }
}






function $4bc68e91bb5385ea$var$_defineProperty(obj, key, value) {
    if (key in obj) Object.defineProperty(obj, key, {
        value: value,
        enumerable: true,
        configurable: true,
        writable: true
    });
    else obj[key] = value;
    return obj;
}
const $4bc68e91bb5385ea$export$2c19dd479e63aadf = "minetrack_favorite_servers";
class $4bc68e91bb5385ea$export$bf46da6bfe361ab1 {
    constructor(app){
        $4bc68e91bb5385ea$var$_defineProperty(this, "handleFavoriteButtonClick", (serverRegistration)=>{
            serverRegistration.isFavorite = !serverRegistration.isFavorite;
            // Update the displayed favorite icon
            document.getElementById(`favorite-toggle_${serverRegistration.serverId}`).setAttribute("class", this.getIconClass(serverRegistration.isFavorite));
            // Request the app controller instantly re-sort the server listing
            // This handles the favorite sorting logic internally
            this._app.sortController.sortServers();
            this._app.graphDisplayManager.handleServerIsFavoriteUpdate(serverRegistration);
            // Write an updated settings payload
            this.updateLocalStorage();
        });
        this._app = app;
    }
    loadLocalStorage() {
        if (typeof localStorage !== "undefined") {
            let serverNames = localStorage.getItem($4bc68e91bb5385ea$export$2c19dd479e63aadf);
            if (serverNames) {
                serverNames = JSON.parse(serverNames);
                for(let i = 0; i < serverNames.length; i++){
                    const serverRegistration = this._app.serverRegistry.getServerRegistration(serverNames[i]);
                    // The serverName may not exist in the backend configuration anymore
                    // Ensure serverRegistration is defined before mutating data or considering valid
                    if (serverRegistration) {
                        serverRegistration.isFavorite = true;
                        // Update icon since by default it is unfavorited
                        document.getElementById(`favorite-toggle_${serverRegistration.serverId}`).setAttribute("class", this.getIconClass(serverRegistration.isFavorite));
                    }
                }
            }
        }
    }
    updateLocalStorage() {
        if (typeof localStorage !== "undefined") {
            // Mutate the serverIds array into server names for storage use
            const serverNames = this._app.serverRegistry.getServerRegistrations().filter((serverRegistration)=>serverRegistration.isFavorite).map((serverRegistration)=>serverRegistration.data.name);
            if (serverNames.length > 0) // Only save if the array contains data, otherwise clear the item
            localStorage.setItem($4bc68e91bb5385ea$export$2c19dd479e63aadf, JSON.stringify(serverNames));
            else localStorage.removeItem($4bc68e91bb5385ea$export$2c19dd479e63aadf);
        }
    }
    getIconClass(isFavorite) {
        if (isFavorite) return "icon-star server-is-favorite";
        else return "icon-star-o server-is-not-favorite";
    }
}


function $76d4b8e5f3ef6cb9$var$_defineProperty(obj, key, value) {
    if (key in obj) Object.defineProperty(obj, key, {
        value: value,
        enumerable: true,
        configurable: true,
        writable: true
    });
    else obj[key] = value;
    return obj;
}
const $76d4b8e5f3ef6cb9$var$HIDDEN_SERVERS_STORAGE_KEY = "minetrack_hidden_servers";
const $76d4b8e5f3ef6cb9$var$SHOW_FAVORITES_STORAGE_KEY = "minetrack_show_favorites";
class $76d4b8e5f3ef6cb9$export$2fa5fa2e8b5ec38b {
    constructor(app){
        $76d4b8e5f3ef6cb9$var$_defineProperty(this, "redraw", ()=>{
            // Use drawing as a hint to update settings
            // This may cause unnessecary localStorage updates, but its a rare and harmless outcome
            this.updateLocalStorage();
            // Copy application state into the series data used by uPlot
            for (const serverRegistration of this._app.serverRegistry.getServerRegistrations())this._plotInstance.series[serverRegistration.getGraphDataIndex()].show = serverRegistration.isVisible;
            this._plotInstance.redraw();
        });
        $76d4b8e5f3ef6cb9$var$_defineProperty(this, "resize", ()=>{
            this._plotInstance.setSize(this.getPlotSize());
            // undefine value so #clearTimeout is not called
            // This is safe even if #resize is manually called since it removes the pending work
            if (this._resizeRequestTimeout) clearTimeout(this._resizeRequestTimeout);
            this._resizeRequestTimeout = undefined;
        });
        $76d4b8e5f3ef6cb9$var$_defineProperty(this, "handleServerButtonClick", (event)=>{
            const serverId = parseInt(event.target.getAttribute("minetrack-server-id"));
            const serverRegistration = this._app.serverRegistry.getServerRegistration(serverId);
            if (serverRegistration.isVisible !== event.target.checked) {
                serverRegistration.isVisible = event.target.checked;
                // Any manual changes automatically disables "Only Favorites" mode
                // Otherwise the auto management might overwrite their manual changes
                this._showOnlyFavorites = false;
                this.redraw();
            }
        });
        $76d4b8e5f3ef6cb9$var$_defineProperty(this, "handleShowButtonClick", (event)=>{
            const showType = event.target.getAttribute("minetrack-show-type");
            // If set to "Only Favorites", set internal state so that
            // visible graphData is automatically updating when a ServerRegistration's #isVisible changes
            // This is also saved and loaded by #loadLocalStorage & #updateLocalStorage
            this._showOnlyFavorites = showType === "favorites";
            let redraw = false;
            this._app.serverRegistry.getServerRegistrations().forEach(function(serverRegistration) {
                let isVisible;
                if (showType === "all") isVisible = true;
                else if (showType === "none") isVisible = false;
                else if (showType === "favorites") isVisible = serverRegistration.isFavorite;
                if (serverRegistration.isVisible !== isVisible) {
                    serverRegistration.isVisible = isVisible;
                    redraw = true;
                }
            });
            if (redraw) {
                this.redraw();
                this.updateCheckboxes();
            }
        });
        $76d4b8e5f3ef6cb9$var$_defineProperty(this, "handleSettingsToggle", ()=>{
            const element = document.getElementById("big-graph-controls-drawer");
            if (element.style.display !== "block") element.style.display = "block";
            else element.style.display = "none";
        });
        $76d4b8e5f3ef6cb9$var$_defineProperty(this, "handleServerIsFavoriteUpdate", (serverRegistration)=>{
            // When in "Only Favorites" mode, visibility is dependent on favorite status
            // Redraw and update elements as needed
            if (this._showOnlyFavorites && serverRegistration.isVisible !== serverRegistration.isFavorite) {
                serverRegistration.isVisible = serverRegistration.isFavorite;
                this.redraw();
                this.updateCheckboxes();
            }
        });
        this._app = app;
        this._graphData = [];
        this._graphTimestamps = [];
        this._hasLoadedSettings = false;
        this._initEventListenersOnce = false;
        this._showOnlyFavorites = false;
    }
    addGraphPoint(timestamp, playerCounts) {
        if (!this._hasLoadedSettings) // _hasLoadedSettings is controlled by #setGraphData
        // It will only be true once the context has been loaded and initial payload received
        // #addGraphPoint should not be called prior to that since it means the data is racing
        // and the application has received updates prior to the initial state
        return;
        // Calculate isZoomed before mutating graphData otherwise the indexed values
        // are out of date and will always fail when compared to plotScaleX.min/max
        const plotScaleX = this._plotInstance.scales.x;
        const isZoomed = plotScaleX.min > this._graphTimestamps[0] || plotScaleX.max < this._graphTimestamps[this._graphTimestamps.length - 1];
        this._graphTimestamps.push(timestamp);
        for(let i = 0; i < playerCounts.length; i++)this._graphData[i].push(playerCounts[i]);
        // Trim all data arrays to only the relevant portion
        // This keeps it in sync with backend data structures
        const graphMaxLength = this._app.publicConfig.graphMaxLength;
        if (this._graphTimestamps.length > graphMaxLength) this._graphTimestamps.splice(0, this._graphTimestamps.length - graphMaxLength);
        for (const series of this._graphData)if (series.length > graphMaxLength) series.splice(0, series.length - graphMaxLength);
        // Avoid redrawing the plot when zoomed
        this._plotInstance.setData(this.getGraphData(), !isZoomed);
    }
    loadLocalStorage() {
        if (typeof localStorage !== "undefined") {
            const showOnlyFavorites = localStorage.getItem($76d4b8e5f3ef6cb9$var$SHOW_FAVORITES_STORAGE_KEY);
            if (showOnlyFavorites) this._showOnlyFavorites = true;
            // If only favorites mode is active, use the stored favorite servers data instead
            let serverNames;
            if (this._showOnlyFavorites) serverNames = localStorage.getItem((0, $4bc68e91bb5385ea$export$2c19dd479e63aadf));
            else serverNames = localStorage.getItem($76d4b8e5f3ef6cb9$var$HIDDEN_SERVERS_STORAGE_KEY);
            if (serverNames) {
                serverNames = JSON.parse(serverNames);
                // Iterate over all active serverRegistrations
                // This merges saved state with current state to prevent desyncs
                for (const serverRegistration of this._app.serverRegistry.getServerRegistrations())// isVisible will be true if showOnlyFavorites && contained in FAVORITE_SERVERS_STORAGE_KEY
                // OR, if it is NOT contains within HIDDEN_SERVERS_STORAGE_KEY
                // Checks between FAVORITE/HIDDEN keys are mutually exclusive
                if (this._showOnlyFavorites) serverRegistration.isVisible = serverNames.indexOf(serverRegistration.data.name) >= 0;
                else serverRegistration.isVisible = serverNames.indexOf(serverRegistration.data.name) < 0;
            }
        }
    }
    updateLocalStorage() {
        if (typeof localStorage !== "undefined") {
            // Mutate the serverIds array into server names for storage use
            const serverNames = this._app.serverRegistry.getServerRegistrations().filter((serverRegistration)=>!serverRegistration.isVisible).map((serverRegistration)=>serverRegistration.data.name);
            // Only store if the array contains data, otherwise clear the item
            // If showOnlyFavorites is true, do NOT store serverNames since the state will be auto managed instead
            if (serverNames.length > 0 && !this._showOnlyFavorites) localStorage.setItem($76d4b8e5f3ef6cb9$var$HIDDEN_SERVERS_STORAGE_KEY, JSON.stringify(serverNames));
            else localStorage.removeItem($76d4b8e5f3ef6cb9$var$HIDDEN_SERVERS_STORAGE_KEY);
            // Only store SHOW_FAVORITES_STORAGE_KEY if true
            if (this._showOnlyFavorites) localStorage.setItem($76d4b8e5f3ef6cb9$var$SHOW_FAVORITES_STORAGE_KEY, true);
            else localStorage.removeItem($76d4b8e5f3ef6cb9$var$SHOW_FAVORITES_STORAGE_KEY);
        }
    }
    getVisibleGraphData() {
        return this._app.serverRegistry.getServerRegistrations().filter((serverRegistration)=>serverRegistration.isVisible).map((serverRegistration)=>this._graphData[serverRegistration.serverId]);
    }
    getPlotSize() {
        return {
            width: Math.max(window.innerWidth, 800) * 0.9,
            height: 400
        };
    }
    getGraphData() {
        return [
            this._graphTimestamps,
            ...this._graphData
        ];
    }
    getGraphDataPoint(serverId, index) {
        const graphData = this._graphData[serverId];
        if (graphData && index < graphData.length && typeof graphData[index] === "number") return graphData[index];
    }
    getClosestPlotSeriesIndex(idx) {
        let closestSeriesIndex = -1;
        let closestSeriesDist = Number.MAX_VALUE;
        const plotHeight = this._plotInstance.bbox.height / devicePixelRatio;
        for(let i = 1; i < this._plotInstance.series.length; i++){
            const series = this._plotInstance.series[i];
            if (!series.show) continue;
            const point = this._plotInstance.data[i][idx];
            if (typeof point === "number") {
                const scale = this._plotInstance.scales[series.scale];
                const posY = (1 - (point - scale.min) / (scale.max - scale.min)) * plotHeight;
                const dist = Math.abs(posY - this._plotInstance.cursor.top);
                if (dist < closestSeriesDist) {
                    closestSeriesIndex = i;
                    closestSeriesDist = dist;
                }
            }
        }
        return closestSeriesIndex;
    }
    buildPlotInstance(timestamps, data) {
        // Lazy load settings from localStorage, if any and if enabled
        if (!this._hasLoadedSettings) {
            this._hasLoadedSettings = true;
            this.loadLocalStorage();
        }
        for (const playerCounts of data){
            // Each playerCounts value corresponds to a ServerRegistration
            // Require each array is the length of timestamps, if not, pad at the start with null values to fit to length
            // This ensures newer ServerRegistrations do not left align due to a lower length
            const lengthDiff = timestamps.length - playerCounts.length;
            if (lengthDiff > 0) {
                const padding = Array(lengthDiff).fill(null);
                playerCounts.unshift(...padding);
            }
        }
        this._graphTimestamps = timestamps;
        this._graphData = data;
        const series = this._app.serverRegistry.getServerRegistrations().map((serverRegistration)=>{
            return {
                stroke: serverRegistration.data.color,
                width: 2,
                value: (_, raw)=>`${(0, $084fcb6f6aaea09e$export$f5dd818bff069720)(raw)}명`,
                show: serverRegistration.isVisible,
                spanGaps: true,
                points: {
                    show: false
                }
            };
        });
        const tickCount = 10;
        const maxFactor = 4;
        // eslint-disable-next-line new-cap
        this._plotInstance = new (0, $516155acbd03f669$export$2e2bcd8739ae039)({
            plugins: [
                (0, $9b3527dabeeb30eb$export$2f2e390e2470b67e)((pos, idx)=>{
                    if (pos) {
                        const closestSeriesIndex = this.getClosestPlotSeriesIndex(idx);
                        const text = this._app.serverRegistry.getServerRegistrations().filter((serverRegistration)=>serverRegistration.isVisible).sort((a, b)=>{
                            if (a.isFavorite !== b.isFavorite) return a.isFavorite ? -1 : 1;
                            else return a.data.name.localeCompare(b.data.name);
                        }).map((serverRegistration)=>{
                            const point = this.getGraphDataPoint(serverRegistration.serverId, idx);
                            let serverName = serverRegistration.data.name;
                            if (closestSeriesIndex === serverRegistration.getGraphDataIndex()) serverName = `<strong>${serverName}</strong>`;
                            if (serverRegistration.isFavorite) serverName = `<span class="${this._app.favoritesManager.getIconClass(true)}"></span> ${serverName}`;
                            return `${serverName}: ${(0, $084fcb6f6aaea09e$export$f5dd818bff069720)(point)}`;
                        }).join("<br>") + `<br><br><strong>${(0, $084fcb6f6aaea09e$export$b144b3a88a3e7934)(this._graphTimestamps[idx])}</strong>`;
                        this._app.tooltip.set(pos.left, pos.top, 10, 10, text);
                    } else this._app.tooltip.hide();
                })
            ],
            ...this.getPlotSize(),
            cursor: {
                y: false
            },
            series: [
                {},
                ...series
            ],
            axes: [
                {
                    font: '14px "Open Sans", sans-serif',
                    stroke: "#FFF",
                    grid: {
                        show: false
                    },
                    space: 60
                },
                {
                    font: '14px "Open Sans", sans-serif',
                    stroke: "#FFF",
                    size: 65,
                    grid: {
                        stroke: "#333",
                        width: 1
                    },
                    split: ()=>{
                        const visibleGraphData = this.getVisibleGraphData();
                        const { scaledMax: scaledMax , scale: scale  } = (0, $16ad6ced048dbe5c$export$4ab14d3905033d17).scaleMatrix(visibleGraphData, tickCount, maxFactor);
                        const ticks = (0, $16ad6ced048dbe5c$export$4ab14d3905033d17).generateTicks(0, scaledMax, scale);
                        return ticks;
                    }
                }
            ],
            scales: {
                y: {
                    auto: false,
                    range: ()=>{
                        const visibleGraphData = this.getVisibleGraphData();
                        const { scaledMin: scaledMin , scaledMax: scaledMax  } = (0, $16ad6ced048dbe5c$export$4ab14d3905033d17).scaleMatrix(visibleGraphData, tickCount, maxFactor);
                        return [
                            scaledMin,
                            scaledMax
                        ];
                    }
                }
            },
            legend: {
                show: false
            }
        }, this.getGraphData(), document.getElementById("big-graph"));
        // Show the settings-toggle element
        document.getElementById("settings-toggle").style.display = "inline-block";
    }
    requestResize() {
        // Only resize when _plotInstance is defined
        // Set a timeout to resize after resize events have not been fired for some duration of time
        // This prevents burning CPU time for multiple, rapid resize events
        if (this._plotInstance) {
            if (this._resizeRequestTimeout) clearTimeout(this._resizeRequestTimeout);
            // Schedule new delayed resize call
            // This can be cancelled by #requestResize, #resize and #reset
            this._resizeRequestTimeout = setTimeout(this.resize, 200);
        }
    }
    initEventListeners() {
        if (!this._initEventListenersOnce) {
            this._initEventListenersOnce = true;
            // These listeners should only be init once since they attach to persistent elements
            document.getElementById("settings-toggle").addEventListener("click", this.handleSettingsToggle, false);
            document.querySelectorAll(".graph-controls-show").forEach((element)=>{
                element.addEventListener("click", this.handleShowButtonClick, false);
            });
        }
        // These listeners should be bound each #initEventListeners call since they are for newly created elements
        document.querySelectorAll(".graph-control").forEach((element)=>{
            element.addEventListener("click", this.handleServerButtonClick, false);
        });
    }
    updateCheckboxes() {
        document.querySelectorAll(".graph-control").forEach((checkbox)=>{
            const serverId = parseInt(checkbox.getAttribute("minetrack-server-id"));
            const serverRegistration = this._app.serverRegistry.getServerRegistration(serverId);
            checkbox.checked = serverRegistration.isVisible;
        });
    }
    reset() {
        // Destroy graphs and unload references
        // uPlot#destroy handles listener de-registration, DOM reset, etc
        if (this._plotInstance) {
            this._plotInstance.destroy();
            this._plotInstance = undefined;
        }
        this._graphTimestamps = [];
        this._graphData = [];
        this._hasLoadedSettings = false;
        // Fire #clearTimeout if the timeout is currently defined
        if (this._resizeRequestTimeout) {
            clearTimeout(this._resizeRequestTimeout);
            this._resizeRequestTimeout = undefined;
        }
        // Reset modified DOM structures
        document.getElementById("big-graph-checkboxes").innerHTML = "";
        document.getElementById("big-graph-controls").style.display = "none";
        document.getElementById("settings-toggle").style.display = "none";
    }
}



function $23d218b875bc8e76$var$_defineProperty(obj, key, value) {
    if (key in obj) Object.defineProperty(obj, key, {
        value: value,
        enumerable: true,
        configurable: true,
        writable: true
    });
    else obj[key] = value;
    return obj;
}
class $23d218b875bc8e76$export$20413a6f534d0a80 {
    constructor(app){
        $23d218b875bc8e76$var$_defineProperty(this, "redraw", ()=>{
            const serverRegistrations = this._app.serverRegistry.getServerRegistrations().sort(function(a, b) {
                return a.playerCount - b.playerCount;
            });
            const totalPlayers = this._app.getTotalPlayerCount();
            let leftPadding = 0;
            for (const serverRegistration of serverRegistrations){
                const width = Math.round(serverRegistration.playerCount / totalPlayers * this._parent.offsetWidth);
                // Update position/width
                // leftPadding is a sum of previous iterations width value
                const div = document.getElementById(`perc-bar-part_${serverRegistration.serverId}`) || this.createPart(serverRegistration);
                const widthPixels = `${width}px`;
                const leftPaddingPixels = `${leftPadding}px`;
                // Only redraw if needed
                if (div.style.width !== widthPixels || div.style.left !== leftPaddingPixels) {
                    div.style.width = widthPixels;
                    div.style.left = leftPaddingPixels;
                }
                leftPadding += width;
            }
        });
        $23d218b875bc8e76$var$_defineProperty(this, "handleMouseOver", (event)=>{
            const serverId = parseInt(event.target.getAttribute("minetrack-server-id"));
            const serverRegistration = this._app.serverRegistry.getServerRegistration(serverId);
            this._app.tooltip.set(event.target.offsetLeft, event.target.offsetTop, 10, this._parent.offsetTop + this._parent.offsetHeight + 10, `${typeof serverRegistration.rankIndex !== "undefined" ? `#${serverRegistration.rankIndex + 1} ` : ""}
      ${serverRegistration.data.name}<br>
      ${(0, $084fcb6f6aaea09e$export$f5dd818bff069720)(serverRegistration.playerCount)} Players<br>
      <strong>${(0, $084fcb6f6aaea09e$export$32e6b1ae352e94d6)(serverRegistration.playerCount, this._app.getTotalPlayerCount())}</strong>`);
        });
        $23d218b875bc8e76$var$_defineProperty(this, "handleMouseOut", ()=>{
            this._app.tooltip.hide();
        });
        this._app = app;
        this._parent = document.getElementById("perc-bar");
    }
    createPart(serverRegistration) {
        const div = document.createElement("div");
        div.id = `perc-bar-part_${serverRegistration.serverId}`;
        div.style.background = serverRegistration.data.color;
        div.setAttribute("class", "perc-bar-part");
        div.setAttribute("minetrack-server-id", serverRegistration.serverId);
        this._parent.appendChild(div);
        // Define events once during creation
        div.addEventListener("mouseover", this.handleMouseOver, false);
        div.addEventListener("mouseout", this.handleMouseOut, false);
        return div;
    }
    reset() {
        // Reset modified DOM elements
        this._parent.innerHTML = "";
    }
}




function $5ec92944edec0500$var$_defineProperty(obj, key, value) {
    if (key in obj) Object.defineProperty(obj, key, {
        value: value,
        enumerable: true,
        configurable: true,
        writable: true
    });
    else obj[key] = value;
    return obj;
}
class $5ec92944edec0500$export$86fbec116b87613f {
    constructor(){
        $5ec92944edec0500$var$_defineProperty(this, "publicConfig", void 0);
        $5ec92944edec0500$var$_defineProperty(this, "addServer", (serverId, payload, timestampPoints)=>{
            // Even if the backend has never pinged the server, the frontend is promised a placeholder object.
            // result = undefined
            // error = defined with "Waiting" description
            // info = safely defined with configured data
            const serverRegistration = this.serverRegistry.createServerRegistration(serverId);
            serverRegistration.initServerStatus(payload);
            // playerCountHistory is only defined when the backend has previous ping data
            // undefined playerCountHistory means this is a placeholder ping generated by the backend
            if (typeof payload.playerCountHistory !== "undefined") {
                // Push the historical data into the graph
                // This will trim and format the data so it is ready for the graph to render once init
                serverRegistration.addGraphPoints(payload.playerCountHistory, timestampPoints);
                // Set initial playerCount to the payload's value
                // This will always exist since it is explicitly generated by the backend
                // This is used for any post-add rendering of things like the percentageBar
                serverRegistration.playerCount = payload.playerCount;
            }
            // Create the plot instance internally with the restructured and cleaned data
            serverRegistration.buildPlotInstance();
            // Handle the last known state (if any) as an incoming update
            // This triggers the main update pipeline and enables centralized update handling
            serverRegistration.updateServerStatus(payload, this.publicConfig.minecraftVersions);
            // Allow the ServerRegistration to bind any DOM events with app instance context
            serverRegistration.initEventListeners();
        });
        $5ec92944edec0500$var$_defineProperty(this, "updateGlobalStats", ()=>{
            // Only redraw when needed
            // These operations are relatively cheap, but the site already does too much rendering
            const totalPlayerCount = this.getTotalPlayerCount();
            if (totalPlayerCount !== this._lastTotalPlayerCount) {
                this._lastTotalPlayerCount = totalPlayerCount;
                document.getElementById("stat_totalPlayers").innerText = (0, $084fcb6f6aaea09e$export$f5dd818bff069720)(totalPlayerCount);
            }
            // Only redraw when needed
            // These operations are relatively cheap, but the site already does too much rendering
            const serverRegistrationCount = this.serverRegistry.getServerRegistrations().length;
            if (serverRegistrationCount !== this._lastServerRegistrationCount) {
                this._lastServerRegistrationCount = serverRegistrationCount;
                document.getElementById("stat_networks").innerText = serverRegistrationCount;
            }
        });
        this.tooltip = new (0, $084fcb6f6aaea09e$export$28c660c63b792dea)();
        this.caption = new (0, $084fcb6f6aaea09e$export$32fbfacc5d962e0c)();
        this.serverRegistry = new (0, $72f844c467870b3a$export$1115b10cc9bb0fa8)(this);
        this.socketManager = new (0, $374c2a3ea73d3d01$export$c91428cbd4f5850d)(this);
        this.sortController = new (0, $4a2c720cc391868f$export$765594e5be71a851)(this);
        this.graphDisplayManager = new (0, $76d4b8e5f3ef6cb9$export$2fa5fa2e8b5ec38b)(this);
        this.percentageBar = new (0, $23d218b875bc8e76$export$20413a6f534d0a80)(this);
        this.favoritesManager = new (0, $4bc68e91bb5385ea$export$bf46da6bfe361ab1)(this);
        this._taskIds = [];
    }
    // Called once the DOM is ready and the app can begin setup
    init() {
        this.socketManager.createWebSocket();
    }
    setPageReady(isReady) {
        document.getElementById("push").style.display = isReady ? "block" : "none";
        document.getElementById("footer").style.display = isReady ? "block" : "none";
        document.getElementById("status-overlay").style.display = isReady ? "none" : "block";
    }
    setPublicConfig(publicConfig) {
        if (publicConfig.graphDurationLabel) publicConfig.graphDurationLabel = this._formatGraphDurationLabel(publicConfig.graphDurationLabel);
        this.publicConfig = publicConfig;
        this.serverRegistry.assignServers(publicConfig.servers);
        // Start repeating frontend tasks once it has received enough data to be considered active
        // This simplifies management logic at the cost of each task needing to safely handle empty data
        this.initTasks();
    }
    _formatGraphDurationLabel(label) {
        // "336h" -> "2주"와 같은 로직 구현
        // 여기서는 간단한 예시로 "h"로 끝나는 경우를 처리합니다.
        const match = label.match(/^(\d+)h$/); // 숫자 + 'h' 패턴 매칭
        if (match) {
            const hours = parseInt(match[1]);
            const days = Math.floor(hours / 24);
            const weeks = Math.floor(days / 7);
            if (weeks > 0) return `${weeks}주`;
            else if (days > 0) return `${days}일`;
            else return `${hours}시간`; // 24시간 미만일 경우 그대로 '시간'으로 표시
        }
        return label; // 매칭되지 않으면 원래 레이블 반환
    }
    handleSyncComplete() {
        this.caption.hide();
        // Load favorites since all servers are registered
        this.favoritesManager.loadLocalStorage();
        // Run a single bulk server sort instead of per-add event since there may be multiple
        this.sortController.show();
        this.percentageBar.redraw();
        // The data may not be there to correctly compute values, but run an attempt
        // Otherwise they will be updated by #initTasks
        this.updateGlobalStats();
    }
    initTasks() {
        this._taskIds.push(setInterval(this.sortController.sortServers, 5000));
    }
    handleDisconnect() {
        this.tooltip.hide();
        // Reset individual tracker elements to flush any held data
        this.serverRegistry.reset();
        this.socketManager.reset();
        this.sortController.reset();
        this.graphDisplayManager.reset();
        this.percentageBar.reset();
        // Undefine publicConfig, resynced during the connection handshake
        this.publicConfig = undefined;
        // Clear all task ids, if any
        this._taskIds.forEach(clearInterval);
        this._taskIds = [];
        // Reset hidden values created by #updateGlobalStats
        this._lastTotalPlayerCount = undefined;
        this._lastServerRegistrationCount = undefined;
        // Reset modified DOM structures
        document.getElementById("stat_totalPlayers").innerText = 0;
        document.getElementById("stat_networks").innerText = 0;
        this.setPageReady(false);
    }
    getTotalPlayerCount() {
        return this.serverRegistry.getServerRegistrations().map((serverRegistration)=>serverRegistration.playerCount).reduce((sum, current)=>sum + current, 0);
    }
}


const $3730becf45741250$var$app = new (0, $5ec92944edec0500$export$86fbec116b87613f)();
document.addEventListener("DOMContentLoaded", ()=>{
    $3730becf45741250$var$app.init();
    window.addEventListener("resize", function() {
        $3730becf45741250$var$app.percentageBar.redraw();
        // Delegate to GraphDisplayManager which can check if the resize is necessary
        $3730becf45741250$var$app.graphDisplayManager.requestResize();
    }, false);
}, false);


//# sourceMappingURL=index.dc50790c.js.map

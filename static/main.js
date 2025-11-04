// === Force dark mode on load ===
document.documentElement.setAttribute('data-theme', 'dark');
document.body.classList.add('dark');

// === Chart setup ===
const isDarkMode = document.body.classList.contains('dark');

const chartOptions1 = {
    layout: {
        background: { type: 'solid', color: isDarkMode ? '#111827' : 'white' },
        textColor: isDarkMode ? '#f3f4f6' : '#1f2937',
        fontFamily: 'Inter, sans-serif',
    },
    grid: {
        vertLines: { color: isDarkMode ? 'rgba(55,65,81,0.5)' : 'rgba(229,231,235,0.8)' },
        horzLines: { color: isDarkMode ? 'rgba(55,65,81,0.5)' : 'rgba(229,231,235,0.8)' },
    },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    timeScale: {
        visible: true,
        borderColor: isDarkMode ? '#374151' : '#e5e7eb',
        timeVisible: true,
    },
    rightPriceScale: { borderColor: isDarkMode ? '#374151' : '#e5e7eb' },
    width: document.getElementById('chart').clientWidth,
    height: document.getElementById('chart').clientHeight,
};

const chartOptions2 = {
    layout: {
        background: { type: 'solid', color: isDarkMode ? '#111827' : 'white' },
        textColor: isDarkMode ? '#f3f4f6' : '#1f2937',
        fontFamily: 'Inter, sans-serif',
    },
    grid: {
        vertLines: { color: isDarkMode ? 'rgba(55,65,81,0.5)' : 'rgba(229,231,235,0.8)' },
        horzLines: { color: isDarkMode ? 'rgba(55,65,81,0.5)' : 'rgba(229,231,235,0.8)' },
    },
    timeScale: {
        visible: true,
        borderColor: isDarkMode ? '#374151' : '#e5e7eb',
        timeVisible: true,
    },
    rightPriceScale: { borderColor: isDarkMode ? '#374151' : '#e5e7eb' },
    width: document.getElementById('chart').clientWidth,
    height: document.getElementById('rsiChart').clientHeight,
};

// === Create charts ===
const chartEl = document.getElementById('chart');
const chart = LightweightCharts.createChart(chartEl, chartOptions1);

// keep your existing series creation (you said changing it breaks other functionality)
const candlestickSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
    upColor: '#26a69a',
    downColor: '#ef5350',
    borderVisible: false,
    wickUpColor: '#26a69a',
    wickDownColor: '#ef5350',
});
const sma5Line = chart.addSeries(LightweightCharts.LineSeries, { color: 'blue', lineWidth: 2 });
const sma20Line = chart.addSeries(LightweightCharts.LineSeries, { color: 'gold', lineWidth: 2 });

const rsiChart = LightweightCharts.createChart(document.getElementById('rsiChart'), chartOptions2);
const rsiLine = rsiChart.addSeries(LightweightCharts.LineSeries, { color: 'green', lineWidth: 2 });
const rsiAvgLine = rsiChart.addSeries(LightweightCharts.LineSeries, { color: 'red', lineWidth: 2 });

// ------------------ MARKERS PLUGIN SETUP (v5 approach) ------------------
// Try to create a series markers plugin using the v5 API exposed on the standalone bundle.
// If it's available, we will use its setMarkers() method. If not, we fall back to
// candlestickSeries.setMarkers (older API) and finally warn if neither is available.

let seriesMarkersApi = null;
if (typeof LightweightCharts.createSeriesMarkers === 'function') {
    try {
        // create an empty markers plugin instance attached to our candlestick series
        seriesMarkersApi = LightweightCharts.createSeriesMarkers(candlestickSeries, []);
        // seriesMarkersApi has setMarkers([...]) and markers() methods per docs
    } catch (err) {
        console.warn('createSeriesMarkers exists but failed to create plugin:', err);
        seriesMarkersApi = null;
    }
} else {
    // no createSeriesMarkers available in this build
    seriesMarkersApi = null;
}

// ------------------ END MARKERS PLUGIN SETUP ------------------

// === Bounded Zoom Logic ===
const timeScale = chart.timeScale();
chart.applyOptions({
    handleScale: { mouseWheel: false, pinch: false },
    handleScroll: { mouseWheel: false, pressedMouseMove: true },
});

const chartContainer = document.getElementById('chart');
let MIN_BARS = 20;   // smallest visible range
let MAX_BARS = 2000; // largest visible range

chartContainer.addEventListener('wheel', (e) => {
    e.preventDefault();
    const zoomIn = e.deltaY < 0;
    const rect = chartContainer.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const mouseLogical = timeScale.coordinateToLogical(mouseX);
    const range = timeScale.getVisibleLogicalRange();
    if (!range || mouseLogical === null) return;

    const totalBars = candlestickSeries.data().length;
    const rangeWidth = range.to - range.from;
    const mouseRatio = (mouseLogical - range.from) / rangeWidth;

    // Calculate new range width with bounds
    let newWidth = zoomIn ? rangeWidth * 0.8 : rangeWidth * 1.25;
    if (newWidth < MIN_BARS) newWidth = MIN_BARS;
    if (newWidth > Math.min(totalBars, MAX_BARS)) newWidth = Math.min(totalBars, MAX_BARS);

    // Stop zooming if at limit
    if ((zoomIn && rangeWidth <= MIN_BARS) || (!zoomIn && rangeWidth >= Math.min(totalBars, MAX_BARS))) {
        return;
    }

    const newFrom = mouseLogical - newWidth * mouseRatio;
    const newTo = newFrom + newWidth;
    timeScale.setVisibleLogicalRange({ from: newFrom, to: newTo });
}, { passive: false });

// === Resize charts ===
window.addEventListener('resize', () => {
    chart.applyOptions({ width: document.getElementById('chart').clientWidth });
    rsiChart.applyOptions({ width: document.getElementById('rsiChart').clientWidth });
});

// === Helper: set markers in a version-robust way ===
function setSeriesMarkers(markersArray) {
    // markersArray expected shape: [{ time: 1670000000, position: 'aboveBar', color: 'green', shape: 'arrowUp', text: 'Buy' }, ...]
    if (seriesMarkersApi && typeof seriesMarkersApi.setMarkers === 'function') {
        // v5 plugin API: setMarkers accepts series marker objects
        try {
            seriesMarkersApi.setMarkers(markersArray);
            return;
        } catch (err) {
            console.warn('seriesMarkersApi.setMarkers failed:', err);
        }
    }

    // fallback: some builds offer a series.setMarkers API (older)
    if (typeof candlestickSeries.setMarkers === 'function') {
        try {
            candlestickSeries.setMarkers(markersArray);
            return;
        } catch (err) {
            console.warn('candlestickSeries.setMarkers failed:', err);
        }
    }

    console.warn('No supported markers API found (createSeriesMarkers / series.setMarkers). Markers were not set.');
}

// === Load NIFTY data ===
async function loadNiftyData(before = null, append = false) {
    const interval = document.getElementById('intervalSelect')?.value || '1m';
    const rsiPeriod = document.getElementById('rsiPeriod')?.value || 9;
    const rsiAvg = document.getElementById('rsiAvg')?.value || 3;
    let url = `/api/data/nifty?interval=${interval}&rsi_period=${rsiPeriod}&rsi_avg=${rsiAvg}`;
    if (before) url += `&before=${before}&limit=1000`;

    const resp = await fetch(url);
    const data = await resp.json();

    // Build a markers array compatible with the docs:
    // v5 expects time to be a timestamp (number) or time object; here backend sends epoch seconds
    const markers = (data.signals || []).map(sig => {
        // createSeriesMarkers accepts either { time: 1670000000 } or { time: { year, month, day } }.
        // We keep epoch seconds (number) — the docs/typings accept numeric times as well.
        // Keep position, color, shape, text fields as-is.
        return {
            time: sig.time,
            position: sig.position || 'aboveBar',
            color: sig.color || (sig.text && sig.text.toLowerCase().includes('buy') ? 'green' : 'red'),
            shape: sig.shape || (sig.text && sig.text.toLowerCase().includes('buy') ? 'arrowUp' : 'arrowDown'),
            text: sig.text || ''
        };
    });

    if (!append) {
        candlestickSeries.setData(data.candlestick);
        sma5Line.setData(data.sma5);
        sma20Line.setData(data.sma20);
        rsiLine.setData(data.rsi_base);
        rsiAvgLine.setData(data.rsi_avg);

        // set markers robustly
        setSeriesMarkers(markers);
    } else {
        const mergeData = (oldData, newData) => {
            const combined = [...newData, ...oldData];
            const seen = new Set();
            return combined.filter(item => {
                if (seen.has(item.time)) return false;
                seen.add(item.time);
                return true;
            });
        };
        candlestickSeries.setData(mergeData(candlestickSeries.data(), data.candlestick));
        sma5Line.setData(mergeData(sma5Line.data(), data.sma5));
        sma20Line.setData(mergeData(sma20Line.data(), data.sma20));
        rsiLine.setData(mergeData(rsiLine.data(), data.rsi_base));
        rsiAvgLine.setData(mergeData(rsiAvgLine.data(), data.rsi_avg));

        // update markers too
        setSeriesMarkers(markers);
    }
}

// === Lazy load older data ===
let isLoading = false;
chart.timeScale().subscribeVisibleLogicalRangeChange(async (newRange) => {
    if (isLoading || !newRange) return;
    const barsInfo = candlestickSeries.barsInLogicalRange(newRange);
    if (!barsInfo || barsInfo.barsBefore < 10) {
        isLoading = true;
        const oldest = candlestickSeries.data()[0].time;
        await loadNiftyData(oldest, true);
        isLoading = false;
    }
});

// === Default Watchlist (no symbol search) ===
function loadWatchlist() {
    const watchlistItems = document.getElementById('watchlistItems');
    watchlistItems.innerHTML = '';
    const niftyItem = document.createElement('div');
    niftyItem.className = 'card bg-base-100 shadow-sm cursor-pointer';
    niftyItem.innerHTML = `
        <div class="card-body p-3">
            <h3 class="font-bold">NIFTY 50</h3>
            <div class="text-xs opacity-70">Data from local files</div>
        </div>
    `;
    niftyItem.addEventListener('click', () => loadNiftyData());
    watchlistItems.appendChild(niftyItem);
}

// === Initial load ===
window.addEventListener('load', () => {
    loadWatchlist();
    loadNiftyData();
});

// === Sync RSI + Price charts ===
function syncVisibleLogicalRange(chart1, chart2) {
    chart1.timeScale().subscribeVisibleLogicalRangeChange(r => chart2.timeScale().setVisibleLogicalRange(r));
    chart2.timeScale().subscribeVisibleLogicalRangeChange(r => chart1.timeScale().setVisibleLogicalRange(r));
}
syncVisibleLogicalRange(chart, rsiChart);

// === Hover legend ===
// ... keep your existing legend code unchanged (not repeated here to avoid clutter)
// If you want, I can paste it back into this file — but I left it as-is per your last message.


// === Hover legend ===
const legend = document.createElement('div');
legend.style.position = 'absolute';
legend.style.left = '12px';
legend.style.top = '12px';
legend.style.zIndex = 10;
legend.style.color = isDarkMode ? '#f3f4f6' : '#111827';
legend.style.fontFamily = 'Inter, sans-serif';
legend.style.fontSize = '12px';
legend.style.backgroundColor = isDarkMode ? 'rgba(17,24,39,0.8)' : 'rgba(255,255,255,0.8)';
legend.style.padding = '6px 10px';
legend.style.borderRadius = '8px';
legend.style.boxShadow = '0 2px 6px rgba(0,0,0,0.2)';
legend.innerHTML = 'Hover a candle…';
document.getElementById('chart').appendChild(legend);

chart.subscribeCrosshairMove(param => {
    if (!param.time) {
        legend.innerHTML = 'Hover a candle…';
        return;
    }

    const candle = param.seriesData.get(candlestickSeries);
    const sma5 = param.seriesData.get(sma5Line);
    const sma20 = param.seriesData.get(sma20Line);
    const date1 = new Date(candle.time * 1000).toLocaleString('en-GB', { timeZone: 'UTC' });

    if (candle) {
        legend.innerHTML = `
            <b>${date1}</b><br>
            O: ${candle.open?.toFixed(2)} H: ${candle.high?.toFixed(2)}<br>
            L: ${candle.low?.toFixed(2)} C: ${candle.close?.toFixed(2)}<br>
            <span style="color:blue">SMA5:</span> ${sma5?.value?.toFixed(2) ?? '–'} &nbsp;
            <span style="color:gold">SMA20:</span> ${sma20?.value?.toFixed(2) ?? '–'}<br>
        `;
    }
});

// === RSI hover legend ===
const rsiLegend = document.createElement('div');
rsiLegend.style.position = 'absolute';
rsiLegend.style.left = '12px';
rsiLegend.style.top = '8px';
rsiLegend.style.zIndex = 10;
rsiLegend.style.color = isDarkMode ? '#f3f4f6' : '#111827';
rsiLegend.style.fontFamily = 'Inter, sans-serif';
rsiLegend.style.fontSize = '12px';
rsiLegend.style.backgroundColor = isDarkMode ? 'rgba(17,24,39,0.8)' : 'rgba(255,255,255,0.8)';
rsiLegend.style.padding = '6px 10px';
rsiLegend.style.borderRadius = '8px';
rsiLegend.style.boxShadow = '0 2px 6px rgba(0,0,0,0.2)';
rsiLegend.innerHTML = 'Hover RSI…';
document.getElementById('rsiChart').appendChild(rsiLegend);

rsiChart.subscribeCrosshairMove(param => {
    if (!param.time) {
        rsiLegend.innerHTML = 'Hover RSI…';
        return;
    }

    const rsi = param.seriesData.get(rsiLine);
    const rsiAvg = param.seriesData.get(rsiAvgLine);
    const rsiPeriod = document.getElementById('rsiPeriod')?.value || 9;
    const rsiAvgLen = document.getElementById('rsiAvg')?.value || 3;
    const date = new Date(param.time * 1000).toLocaleString('en-GB', { timeZone: 'UTC' });

    if (rsi && rsiAvg) {
        rsiLegend.innerHTML = `
            <b>${date}</b><br>
            <span style="color:green">RSI(${rsiPeriod}):</span> ${rsi.value?.toFixed(2) ?? '–'} &nbsp;
            <span style="color:red">Avg(${rsiAvgLen}):</span> ${rsiAvg.value?.toFixed(2) ?? '–'}
        `;
    }
});

// === Theme toggle ===
const themeToggle = document.getElementById('themeToggle');
if (themeToggle) {
    themeToggle.addEventListener('click', () => {
        const isDark = document.body.classList.toggle('dark');
        document.documentElement.setAttribute('data-theme', isDark ? 'dark' : 'light');
        const chartColor = isDark ? '#111827' : 'white';
        const textColor = isDark ? '#f3f4f6' : '#1f2937';
        const gridColor = isDark ? 'rgba(55,65,81,0.5)' : 'rgba(229,231,235,0.8)';
        chart.applyOptions({
            layout: { background: { color: chartColor }, textColor },
            grid: { vertLines: { color: gridColor }, horzLines: { color: gridColor } },
        });
        rsiChart.applyOptions({
            layout: { background: { color: chartColor }, textColor },
            grid: { vertLines: { color: gridColor }, horzLines: { color: gridColor } },
        });
    });
}

// === Wire UI controls ===
const fetchBtn = document.getElementById('fetchData');
if (fetchBtn) fetchBtn.addEventListener('click', () => loadNiftyData());

const intervalSelect = document.getElementById('intervalSelect');
if (intervalSelect) intervalSelect.addEventListener('change', () => loadNiftyData());

const applyRsi = document.getElementById('applyRsi');
if (applyRsi) applyRsi.addEventListener('click', () => loadNiftyData());

// === Initial load ===
window.addEventListener('load', () => {
    loadNiftyData();
});

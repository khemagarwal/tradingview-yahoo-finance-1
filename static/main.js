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
const rsiLine = rsiChart.addSeries(LightweightCharts.LineSeries, { color: 'red', lineWidth: 2 });

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

// === Load NIFTY data ===
async function loadNiftyData(before = null, append = false) {
    const interval = document.getElementById('intervalSelect')?.value || '1m';
    let url = `/api/data/nifty?interval=${interval}`;
    if (before) url += `&before=${before}&limit=1000`;

    const resp = await fetch(url);
    const data = await resp.json();


    if (!append) {
        // Initial load
        candlestickSeries.setData(data.candlestick);
        sma5Line.setData(data.sma5);
        sma20Line.setData(data.sma20);
        rsiLine.setData(data.rsi);
    } else {
        // Append older data while preserving continuity
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
        rsiLine.setData(mergeData(rsiLine.data(), data.rsi));
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
            <span style="color:gold">SMA20:</span> ${sma20?.value?.toFixed(2) ?? '–'}
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

// === Wire UI controls: Fetch button + Interval select ===
const fetchBtn = document.getElementById('fetchData');
if (fetchBtn) {
    fetchBtn.addEventListener('click', () => loadNiftyData());
}

const intervalSelect = document.getElementById('intervalSelect');
if (intervalSelect) {
    intervalSelect.addEventListener('change', () => loadNiftyData());
}

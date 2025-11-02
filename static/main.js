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
        secondsVisible: false,
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
        secondsVisible: false,
    },
    rightPriceScale: { borderColor: isDarkMode ? '#374151' : '#e5e7eb' },
    width: document.getElementById('chart').clientWidth,
    height: document.getElementById('rsiChart').clientHeight,
};

// === Create charts ===
const chart = LightweightCharts.createChart(document.getElementById('chart'), chartOptions1);
const candlestickSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
    upColor: '#26a69a',
    downColor: '#ef5350',
    borderVisible: false,
    wickUpColor: '#26a69a',
    wickDownColor: '#ef5350',
});

const sma5Line = chart.addSeries(LightweightCharts.LineSeries, { color: 'blue', lineWidth: 2, priceLineVisible: false });
const sma20Line = chart.addSeries(LightweightCharts.LineSeries, { color: 'gold', lineWidth: 2, priceLineVisible: false });

const rsiChart = LightweightCharts.createChart(document.getElementById('rsiChart'), chartOptions2);
const rsiLine = rsiChart.addSeries(LightweightCharts.LineSeries, { color: 'red', lineWidth: 2, priceLineVisible: false });

// === Resize charts on window change ===
window.addEventListener('resize', () => {
    chart.applyOptions({ width: document.getElementById('chart').clientWidth });
    rsiChart.applyOptions({ width: document.getElementById('rsiChart').clientWidth });
});

// === Load NIFTY 50 data ===
async function loadNiftyData(before = null, append = false) {
    const interval = document.getElementById('intervalSelect')?.value || '1m';
    let url = `/api/data/nifty?interval=${interval}`;
    if (before) url += `&before=${before}&limit=1000`;

    const resp = await fetch(url);
    const data = await resp.json();

    if (!append) {
        candlestickSeries.setData(data.candlestick);
        sma5Line.setData(data.sma5);
        sma20Line.setData(data.sma20);
        rsiLine.setData(data.rsi);
    } else {
        const currentData = candlestickSeries.data();
        candlestickSeries.setData([...data.candlestick, ...currentData]);
        sma5Line.setData([...data.sma5, ...sma5Line.data()]);
        sma20Line.setData([...data.sma20, ...sma20Line.data()]);
        rsiLine.setData([...data.rsi, ...rsiLine.data()]);
    }
}

// === Lazy loading: load more data when scrolling left ===
let isLoading = false;
chart.timeScale().subscribeVisibleLogicalRangeChange(async (newRange) => {
    if (isLoading || !newRange) return;

    const barsInfo = candlestickSeries.barsInLogicalRange(newRange);
    if (!barsInfo || barsInfo.barsBefore < 10) {
        isLoading = true;
        const currentData = candlestickSeries.data();
        if (!currentData || currentData.length === 0) return;
        const oldest = currentData[0].time;

        console.log("⏳ Loading older candles before", oldest);
        await loadNiftyData(oldest, true);
        isLoading = false;
    }
});

// === Watchlist ===
function loadWatchlist() {
    const watchlistItems = document.getElementById('watchlistItems');
    watchlistItems.innerHTML = '';

    const niftyItem = document.createElement('div');
    niftyItem.className = 'card bg-base-100 hover:bg-base-200 shadow-sm hover:shadow cursor-pointer transition-all';
    niftyItem.innerHTML = `
        <div class="card-body p-3">
            <h3 class="font-bold">NIFTY 50</h3>
            <div class="text-xs opacity-70">1-Minute Interval</div>
        </div>
    `;
    niftyItem.addEventListener('click', () => loadNiftyData());
    watchlistItems.appendChild(niftyItem);
}

// === Initial Load ===
window.addEventListener('load', () => {
    loadWatchlist();
    loadNiftyData();

    const fetchBtn = document.getElementById('fetchBtn');
    if (fetchBtn) {
        fetchBtn.addEventListener('click', () => loadNiftyData());
    }
});

// === Sync RSI + Price charts ===
function syncVisibleLogicalRange(chart1, chart2) {
    chart1.timeScale().subscribeVisibleLogicalRangeChange(timeRange => {
        chart2.timeScale().setVisibleLogicalRange(timeRange);
    });
    chart2.timeScale().subscribeVisibleLogicalRangeChange(timeRange => {
        chart1.timeScale().setVisibleLogicalRange(timeRange);
    });
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
    if (candle) {
        legend.innerHTML = `
            <b>${new Date(param.time * 1000).toLocaleString()}</b><br>
            O: ${candle.open?.toFixed(2)} H: ${candle.high?.toFixed(2)}<br>
            L: ${candle.low?.toFixed(2)} C: ${candle.close?.toFixed(2)}<br>
            <span style="color:blue">SMA5:</span> ${sma5?.value?.toFixed(2) ?? '–'} &nbsp;
            <span style="color:gold">SMA20:</span> ${sma20?.value?.toFixed(2) ?? '–'}
        `;
    }
});

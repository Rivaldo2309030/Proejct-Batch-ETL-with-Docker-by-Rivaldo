import streamlit as st
from pymongo import MongoClient
import pandas as pd
import altair as alt
from datetime import datetime, timezone

# --- Page config with emoji and wide layout ---
st.set_page_config(page_title="🌎 Global Data Dashboard", page_icon="🌟", layout="wide")

# --- Custom CSS for colors, fonts, and styles ---
st.markdown(
    """
    <style>
    /* Background gradient */
    .main {
        background: linear-gradient(135deg, #6a11cb 0%, #2575fc 100%);
        color: white;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    /* Center header */
    h1 {
        text-align: center;
        font-weight: 900;
        font-size: 3.5rem;
        text-shadow: 2px 2px 5px #000000a0;
        margin-bottom: 0.1em;
    }
    /* Subtitle style */
    .subtitle {
        text-align: center;
        font-size: 1.2rem;
        margin-bottom: 2em;
        color: #e0e0e0;
        font-weight: 600;
        text-shadow: 1px 1px 3px #00000070;
    }
    /* Sidebar title */
    .sidebar .sidebar-content {
        background: #1f2937;
        color: #f0f0f0;
        font-weight: 700;
        font-size: 1.1rem;
    }
    /* Metric colors */
    .metric-label {
        color: #ffd700 !important;  /* gold */
        font-weight: 700;
    }
    /* Dataframe styling */
    div[data-testid="stDataFrame"] {
        background-color: #ffffff20 !important;
        border-radius: 8px;
    }
    /* Altair tooltip styling */
    .vega-tooltip {
        background-color: #333 !important;
        color: #eee !important;
        font-weight: 600 !important;
        border-radius: 5px !important;
        box-shadow: 0 0 5px #0009 !important;
    }
    </style>
    """, unsafe_allow_html=True
)

# --- Page header ---
st.markdown("<h1>🌎 Global Data Dashboard</h1>", unsafe_allow_html=True)
st.markdown('<p class="subtitle">Interactive visualization of countries, COVID-19 and weather data — updated every 24 hours.</p>', unsafe_allow_html=True)

# --- MongoDB connection ---
@st.cache_resource
def get_mongo_client():
    return MongoClient("mongodb://host.docker.internal:27017")

client = get_mongo_client()
db_countries = client["countries_db"]
db_covid = client["covid_db"]
db_weather = client["weather_db"]

# --- Sidebar ---
with st.sidebar:
    st.markdown("<h2 style='color:#ffd700;'>🎛️ Filters</h2>", unsafe_allow_html=True)

    # Countries filter
    st.markdown("### 🌍 Regions")
    regions = sorted(db_countries.processed_countries.distinct("region"))
    region_filter = st.selectbox("Select region", options=["All"] + regions)

    st.markdown("### 🏙️ Population density")
    min_density, max_density = st.slider("Density range", 0, 1000, (0, 1000))

    st.markdown("---")

    # COVID filter
    st.markdown("### 🦠 COVID-19")
    covid_countries = st.multiselect("Select countries", options=sorted(db_covid.processed_covid.distinct("country")))

    st.markdown("---")

    # Weather filter
    st.markdown("### 🌦️ Weather Date")
    latest_weather = db_weather.processed_weather.find_one(sort=[("summary_time", -1)])
    max_date = datetime.now().date()
    if latest_weather:
        try:
            max_date = datetime.fromisoformat(latest_weather["summary_time"]).date()
        except Exception:
            pass
    date_filter = st.date_input("Select date", max_date, max_value=max_date)

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["🌍 Countries", "🦠 COVID-19", "🌦️ Weather"])

def format_number(x):
    try:
        return f"{int(x):,}"
    except:
        return "N/A"

with tab1:
    st.markdown("### 🌍 Countries Information")

    countries_cursor = db_countries.processed_countries.find()
    countries_list = list(countries_cursor)
    for doc in countries_list:
        doc["_id"] = str(doc["_id"])
    countries_df = pd.DataFrame(countries_list)

    if region_filter != "All":
        countries_df = countries_df[countries_df["region"] == region_filter]

    countries_df["density"] = countries_df.apply(
        lambda r: r["population"] / r["area"] if r.get("area") and r["area"] > 0 else 0,
        axis=1
    )
    countries_df = countries_df[(countries_df["density"] >= min_density) & (countries_df["density"] <= max_density)]

    latest_summary = db_countries.country_summaries.find_one(sort=[("summary_time", -1)])
    if latest_summary:
        try:
            updated_str = datetime.fromisoformat(latest_summary["summary_time"]).strftime("%d %b %Y, %H:%M:%S")
            st.caption(f"🕒 Last update: {updated_str}")
        except Exception:
            pass

    if countries_df.empty:
        st.warning("⚠️ No countries found with current filters.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("🌐 Countries", len(countries_df))
        col2.metric("👥 Total Population", format_number(countries_df['population'].sum()))
        col3.metric("📏 Avg Density", f"{countries_df['density'].mean():.1f}")

        st.markdown("### 🧾 Data Table")
        st.dataframe(countries_df, use_container_width=True)

        st.download_button("📥 Download CSV", countries_df.to_csv(index=False).encode(), "countries_filtered.csv")

        st.markdown("### 📊 Population by Country")
        bar_chart = alt.Chart(countries_df).mark_bar(color="#F39C12").encode(
            x=alt.X('name', sort='-y'),
            y='population',
            tooltip=['name', 'population']
        ).properties(width=800, height=400)
        st.altair_chart(bar_chart, use_container_width=True)

        st.markdown("### 🧮 Area vs Density")
        scatter_chart = alt.Chart(countries_df).mark_circle(size=80).encode(
            x='area',
            y='density',
            color=alt.Color('region', legend=None, scale=alt.Scale(scheme='category10')),
            tooltip=['name', 'area', 'density']
        ).properties(width=800, height=400)
        st.altair_chart(scatter_chart, use_container_width=True)

        # Pie chart: distribution of countries by region
        region_counts = countries_df['region'].value_counts().reset_index()
        region_counts.columns = ['region', 'count']

        st.markdown("### 🥧 Countries Distribution by Region")
        pie_chart = alt.Chart(region_counts).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="count", type="quantitative"),
            color=alt.Color(field="region", type="nominal", legend=alt.Legend(title="Region")),
            tooltip=["region", "count"]
        ).properties(width=400, height=400)
        st.altair_chart(pie_chart, use_container_width=False)

with tab2:
    st.markdown("### 🦠 COVID-19 Stats")

    query_covid = {"country": {"$in": covid_countries}} if covid_countries else {}
    covid_cursor = db_covid.processed_covid.find(query_covid)
    covid_list = list(covid_cursor)
    for doc in covid_list:
        doc["_id"] = str(doc["_id"])
    covid_df = pd.DataFrame(covid_list)

    if covid_df.empty:
        st.warning("⚠️ No COVID-19 data for selected filters.")
    else:
        if len(covid_df) == 1:
            row = covid_df.iloc[0]
            try:
                updated_dt = datetime.fromisoformat(row["updated_at"]).replace(tzinfo=timezone.utc).astimezone()
                updated_str = updated_dt.strftime("%d %b %Y, %H:%M:%S")
                diff = datetime.now().astimezone() - updated_dt
            except Exception:
                updated_str = "Unknown"
                diff = None

            def natural_time(diff):
                if diff is None:
                    return "Unknown time"
                seconds = int(diff.total_seconds())
                if seconds < 60:
                    return f"{seconds} seconds"
                minutes = seconds // 60
                if minutes < 60:
                    return f"{minutes} minutes"
                hours = minutes // 60
                if hours < 24:
                    return f"{hours} hours"
                days = hours // 24
                return f"{days} days"

            st.subheader(f"📍 {row['country']}")
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("🟠 Active", format_number(row['active']))
            col2.metric("🧪 Cases", format_number(row['cases']))
            col3.metric("⚰️ Deaths", format_number(row['deaths']), delta_color="inverse")
            col4.metric("💚 Recovered", format_number(row['recovered']))
            col5.metric("📉 Fatality Rate", f"{row['fatality_rate']:.2%}" if row.get('fatality_rate') is not None else "N/A")

            st.caption(f"🕒 Last updated: {updated_str} ({natural_time(diff)} ago)")

            summary_df = pd.DataFrame({
                "Status": ["Active", "Deaths", "Recovered"],
                "Count": [row['active'], row['deaths'], row['recovered']]
            })

            st.markdown("### 📊 Status Summary")
            bar = alt.Chart(summary_df).mark_bar().encode(
                x='Status',
                y='Count',
                color=alt.Color('Status', scale=alt.Scale(domain=["Active", "Deaths", "Recovered"],
                                                          range=["#f39c12", "#e74c3c", "#2ecc71"])),
                tooltip=['Status', 'Count']
            ).properties(width=600, height=300)
            st.altair_chart(bar, use_container_width=True)

        else:
            st.markdown("### 📊 General Indicators")
            col1, col2, col3 = st.columns(3)
            col1.metric("Cases", format_number(covid_df['cases'].sum()))
            col2.metric("Deaths", format_number(covid_df['deaths'].sum()))
            fatality_mean = covid_df['fatality_rate'].mean() if not covid_df['fatality_rate'].isnull().all() else None
            col3.metric("Fatality Rate", f"{fatality_mean:.2%}" if fatality_mean is not None else "N/A")

            st.dataframe(covid_df, use_container_width=True)

            st.markdown("### 📈 Cases by Country")
            covid_bar = alt.Chart(covid_df).mark_bar(color="#3498db").encode(
                x=alt.X('country', sort='-y'),
                y='cases',
                tooltip=['country', 'cases', 'deaths']
            ).properties(width=800, height=400)
            st.altair_chart(covid_bar, use_container_width=True)

            st.markdown("### 💚 Recovered by Country")
            recovery_chart = alt.Chart(covid_df).mark_bar(color='#2ecc71').encode(
                x='country',
                y='recovered',
                tooltip=['country', 'recovered']
            ).properties(width=800, height=400)
            st.altair_chart(recovery_chart, use_container_width=True)

            # Stacked bar chart for active, deaths and recovered
            stacked_df = covid_df.melt(id_vars=["country"], value_vars=["active", "deaths", "recovered"],
                                       var_name="status", value_name="count")

            st.markdown("### 📊 COVID Status Breakdown by Country")
            stacked_bar = alt.Chart(stacked_df).mark_bar().encode(
                x=alt.X('country', sort='-y'),
                y='count',
                color=alt.Color('status', scale=alt.Scale(domain=["active", "deaths", "recovered"],
                                                         range=["#f39c12", "#e74c3c", "#2ecc71"]),
                                legend=alt.Legend(title="Status")),
                tooltip=['country', 'status', 'count']
            ).properties(width=800, height=400)
            st.altair_chart(stacked_bar, use_container_width=True)

with tab3:
    st.markdown("### 🌦️ Weather Information")

    date_prefix = date_filter.strftime("%Y-%m-%d")
    weather_doc = db_weather.processed_weather.find_one({"summary_time": {"$regex": f"^{date_prefix}"}})

    if weather_doc:
        st.caption(f"🕒 Last updated: {weather_doc.get('summary_time')}")

        records = weather_doc.get("records", [])
        for rec in records:
            rec["_id"] = str(rec.get("_id", ""))

        weather_df = pd.DataFrame(records)
        weather_df["time"] = pd.to_datetime(weather_df["time"]).dt.tz_localize(None)
        weather_df = weather_df[weather_df["time"].dt.date == date_filter].reset_index(drop=True)

        st.subheader(f"📅 Weather for {date_filter.strftime('%d/%m/%Y')}")

        col1, col2 = st.columns(2)
        col1.metric("🌡️ Avg Temp °C", f"{weather_df['temperature_c'].mean():.1f}")
        col2.metric("🌧️ Max Rain mm", f"{weather_df['rain_mm'].max():.1f}")

        st.markdown("### 🧾 Weather Records")
        st.dataframe(weather_df, use_container_width=True)

        st.markdown("### 📈 Temperature Throughout the Day")
        temp_line = alt.Chart(weather_df).mark_line(color="#f39c12").encode(
            x='time',
            y='temperature_c',
            tooltip=['time', 'temperature_c']
        ).properties(width=800, height=400)
        st.altair_chart(temp_line, use_container_width=True)

        # Area chart for accumulated rainfall
        st.markdown("### 🌧️ Accumulated Rainfall Throughout the Day")
        rain_area = alt.Chart(weather_df).mark_area(color="#3498db", opacity=0.6).encode(
            x='time',
            y='rain_mm',
            tooltip=['time', 'rain_mm']
        ).properties(width=800, height=400)
        st.altair_chart(rain_area, use_container_width=True)

    else:
        st.warning("⚠️ No data available for the selected date.")

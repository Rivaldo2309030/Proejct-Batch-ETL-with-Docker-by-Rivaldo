import streamlit as st
from pymongo import MongoClient
import pandas as pd
import altair as alt
from datetime import datetime, timezone

# --- MongoDB connection ---
@st.cache_resource
def get_mongo_client():
    return MongoClient("mongodb://host.docker.internal:27017")

client = get_mongo_client()

db_countries = client["countries_db"]
db_covid = client["covid_db"]
db_weather = client["weather_db"]

# Sidebar filters
st.sidebar.title("Filters")

# Countries filter
region_filter = st.sidebar.selectbox(
    "Region (Countries)",
    options=["All"] + sorted(db_countries.processed_countries.distinct("region"))
)
min_density = st.sidebar.slider("Minimum Density", 0, 1000, 0)
max_density = st.sidebar.slider("Maximum Density", 0, 1000, 1000)

# COVID filter
st.sidebar.markdown("---")
covid_countries = st.sidebar.multiselect(
    "COVID Countries (Multi-select)",
    options=sorted(db_covid.processed_covid.distinct("country")),
    default=[]
)

# Weather filter
st.sidebar.markdown("---")
latest_weather = db_weather.processed_weather.find_one(sort=[("summary_time", -1)])
if latest_weather:
    max_date = datetime.fromisoformat(latest_weather["summary_time"]).date()
else:
    max_date = datetime.now().date()

date_filter = st.sidebar.date_input("Weather Date", max_date)

# Tabs
tab1, tab2, tab3 = st.tabs(["🌍 Countries", "🦠 COVID", "🌦️ Weather"])

# --- Countries tab ---
with tab1:
    countries_cursor = db_countries.processed_countries.find()
    countries_list = list(countries_cursor)
    # Convert ObjectId to string to avoid pyarrow errors
    for doc in countries_list:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
    countries_df = pd.DataFrame(countries_list)

    # Filter by region if selected
    if region_filter != "All":
        countries_df = countries_df[countries_df["region"] == region_filter]

    # Calculate population density (population / area), avoid division by zero
    countries_df["density"] = countries_df.apply(
        lambda row: row["population"] / row["area"] if row.get("area") and row["area"] > 0 else 0,
        axis=1
    )

    # Filter by density range
    countries_df = countries_df[
        (countries_df["density"] >= min_density) & (countries_df["density"] <= max_density)
    ]

    st.markdown("ℹ️ **Country data automatically updates every 24 hours.**")

    # Show last update time if available
    latest_summary = db_countries.country_summaries.find_one(sort=[("summary_time", -1)])
    if latest_summary:
        updated_str = datetime.fromisoformat(latest_summary["summary_time"]).strftime("%d %b %Y, %H:%M:%S")
        st.caption(f"Last countries update: {updated_str}")

    if countries_df.empty:
        st.warning("No countries match these filters.")
    else:
        st.subheader("KPIs")
        col1, col2, col3 = st.columns(3)
        col1.metric("Filtered Countries", len(countries_df))
        col2.metric("Total Population", f"{int(countries_df['population'].sum()):,}")
        col3.metric("Average Density", f"{countries_df['density'].mean():.1f}")

        st.dataframe(countries_df)

        st.download_button(
            "Download CSV",
            countries_df.to_csv(index=False).encode(),
            file_name="countries_filtered.csv",
            mime="text/csv"
        )

        bar_chart = alt.Chart(countries_df).mark_bar().encode(
            x=alt.X('name', sort='-y'),
            y='population',
            tooltip=['name', 'population']
        ).properties(width=700, height=400, title="Population by Country")
        st.altair_chart(bar_chart, use_container_width=True)

        scatter_chart = alt.Chart(countries_df).mark_circle(size=60).encode(
            x='area',
            y='density',
            color='region',
            tooltip=['name', 'area', 'density']
        ).properties(width=700, height=400, title="Area vs Density")
        st.altair_chart(scatter_chart, use_container_width=True)

# --- COVID tab ---
with tab2:
    query_covid = {}
    if covid_countries:
        query_covid["country"] = {"$in": covid_countries}

    covid_cursor = db_covid.processed_covid.find(query_covid)
    covid_list = list(covid_cursor)
    # Convert ObjectId to string to avoid pyarrow errors
    for doc in covid_list:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
    covid_df = pd.DataFrame(covid_list)

    st.markdown("ℹ️ **COVID data automatically updates every 24 hours.**")

    if covid_df.empty:
        st.warning("No COVID data matches these filters.")
    else:
        if len(covid_df) == 1:
            row = covid_df.iloc[0]

            updated_dt = datetime.fromisoformat(row["updated_at"]).replace(tzinfo=timezone.utc).astimezone()
            updated_str = updated_dt.strftime("%d %b %Y, %H:%M:%S")

            now_local = datetime.now().astimezone()
            diff = now_local - updated_dt

            def natural_time(diff):
                seconds = int(diff.total_seconds())
                if seconds < 60:
                    return f"{seconds} seconds ago"
                minutes = seconds // 60
                if minutes < 60:
                    return f"{minutes} minutes ago"
                hours = minutes // 60
                if hours < 24:
                    return f"{hours} hours ago"
                days = hours // 24
                return f"{days} days ago"

            updated_ago = natural_time(diff)

            st.subheader(f"COVID Data for {row['country']}")
            col1, col2, col3, col4, col5 = st.columns(5)

            col1.metric("Active Cases", f"{row['active']:,}", help="Currently infected individuals")
            col2.metric("Total Cases", f"{row['cases']:,}", help="Total confirmed cases")
            col3.metric("Deaths", f"{row['deaths']:,}", help="Total deaths", delta_color="inverse")
            col4.metric("Recovered", f"{row['recovered']:,}", help="Successfully recovered individuals")
            col5.metric("Fatality Rate", f"{row['fatality_rate']:.2%}", help="Deaths over confirmed cases percentage")

            st.markdown(f"**Last updated:** {updated_str} ({updated_ago})")

            summary_df = pd.DataFrame({
                "Status": ["Active", "Deaths", "Recovered"],
                "Count": [row['active'], row['deaths'], row['recovered']]
            })

            bar = alt.Chart(summary_df).mark_bar().encode(
                x=alt.X('Status', sort=None),
                y='Count',
                color=alt.Color('Status', scale=alt.Scale(domain=["Active", "Deaths", "Recovered"], range=["orange", "red", "green"])),
                tooltip=['Status', 'Count']
            ).properties(width=600, height=300, title="COVID Summary")

            st.altair_chart(bar, use_container_width=True)

        else:
            st.subheader("KPIs")
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Cases", f"{covid_df['cases'].sum():,}")
            col2.metric("Total Deaths", f"{covid_df['deaths'].sum():,}")
            col3.metric("Avg. Fatality Rate", f"{covid_df['fatality_rate'].mean():.2%}")

            st.dataframe(covid_df)

            covid_bar = alt.Chart(covid_df).mark_bar().encode(
                x=alt.X('country', sort='-y'),
                y='cases',
                tooltip=['country', 'cases', 'deaths']
            ).properties(width=700, height=400, title="Cases by Country")
            st.altair_chart(covid_bar, use_container_width=True)

            recovery_chart = alt.Chart(covid_df).mark_bar(color='green').encode(
                x='country',
                y='recovered',
                tooltip=['country', 'recovered']
            ).properties(width=700, height=400, title="Recovered by Country")
            st.altair_chart(recovery_chart, use_container_width=True)

# --- Weather tab ---
with tab3:
    date_prefix = date_filter.strftime("%Y-%m-%d")

    weather_doc = db_weather.processed_weather.find_one({
        "summary_time": {"$regex": f"^{date_prefix}"}
    })

    st.markdown("ℹ️ **Weather data automatically updates every 24 hours.**")

    if weather_doc:
        records = weather_doc.get("records", [])
        # Convert ObjectId to string if present in records
        for rec in records:
            if "_id" in rec:
                rec["_id"] = str(rec["_id"])
        weather_df = pd.DataFrame(records)

        st.caption(f"Last update: {weather_doc.get('summary_time')}")

        weather_df["time"] = pd.to_datetime(weather_df["time"]).dt.tz_localize(None)
        weather_df = weather_df[weather_df["time"].dt.date == date_filter]
        weather_df = weather_df.reset_index(drop=True)

        st.subheader(f"Weather - {date_filter}")

        col1, col2 = st.columns(2)
        col1.metric("Avg. Temp °C", f"{weather_df['temperature_c'].mean():.1f}")
        col2.metric("Max Rain mm", f"{weather_df['rain_mm'].max():.1f}")

        st.dataframe(weather_df)

        temp_line = alt.Chart(weather_df).mark_line().encode(
            x='time',
            y='temperature_c',
            tooltip=['time', 'temperature_c']
        ).properties(width=700, height=400, title="Temperature Over the Day")
        st.altair_chart(temp_line, use_container_width=True)

    else:
        st.warning("No weather data available for the selected date.")

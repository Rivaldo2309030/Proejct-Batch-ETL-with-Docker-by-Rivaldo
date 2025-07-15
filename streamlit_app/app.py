import streamlit as st
from pymongo import MongoClient
import pandas as pd
import altair as alt
from datetime import datetime, timezone

# --- Conexión MongoDB ---
@st.cache_resource
def get_mongo_client():
    return MongoClient("mongodb://host.docker.internal:27017")

client = get_mongo_client()

db_countries = client["countries_db"]
db_covid = client["covid_db"]
db_weather = client["weather_db"]

# Sidebar filtros
st.sidebar.title("Filtros")

# Países
region_filter = st.sidebar.selectbox(
    "Región (Países)",
    options=["All"] + sorted(db_countries.processed_countries.distinct("region"))
)
min_density = st.sidebar.slider("Densidad mínima", 0, 1000, 0)
max_density = st.sidebar.slider("Densidad máxima", 0, 1000, 1000)

# COVID
st.sidebar.markdown("---")
covid_countries = st.sidebar.multiselect(
    "Países COVID (Multi-selección)",
    options=sorted(db_covid.processed_covid.distinct("country")),
    default=[]
)

# Weather
st.sidebar.markdown("---")
latest_weather = db_weather.processed_weather.find_one(sort=[("summary_time", -1)])
if latest_weather:
    max_date = datetime.fromisoformat(latest_weather["summary_time"]).date()
else:
    max_date = datetime.now().date()

date_filter = st.sidebar.date_input("Fecha de clima", max_date)

# Tabs
tab1, tab2, tab3 = st.tabs(["🌍 Países", "🦠 COVID", "🌦️ Clima"])

# --- Países ---
with tab1:
    countries_cursor = db_countries.processed_countries.find()
    countries_list = list(countries_cursor)
    # Convertir ObjectId a string para evitar error en pyarrow
    for doc in countries_list:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
    countries_df = pd.DataFrame(countries_list)

    # Filtrar región si aplica
    if region_filter != "All":
        countries_df = countries_df[countries_df["region"] == region_filter]

    # Calcular densidad poblacional (población / área), cuidado con división por cero
    countries_df["density"] = countries_df.apply(
        lambda row: row["population"] / row["area"] if row.get("area") and row["area"] > 0 else 0,
        axis=1
    )

    # Filtrar por densidad
    countries_df = countries_df[
        (countries_df["density"] >= min_density) & (countries_df["density"] <= max_density)
    ]

    st.markdown("ℹ️ **Los datos de países se actualizan automáticamente cada 24 horas.**")

    # Mostrar fecha de última actualización si hay
    latest_summary = db_countries.country_summaries.find_one(sort=[("summary_time", -1)])
    if latest_summary:
        updated_str = datetime.fromisoformat(latest_summary["summary_time"]).strftime("%d %b %Y, %H:%M:%S")
        st.caption(f"Última actualización de países: {updated_str}")

    if countries_df.empty:
        st.warning("No hay países con esos filtros.")
    else:
        st.subheader("KPIs")
        col1, col2, col3 = st.columns(3)
        col1.metric("Países filtrados", len(countries_df))
        col2.metric("Población total", f"{int(countries_df['population'].sum()):,}")
        col3.metric("Densidad promedio", f"{countries_df['density'].mean():.1f}")

        st.dataframe(countries_df)

        st.download_button(
            "Descargar CSV",
            countries_df.to_csv(index=False).encode(),
            file_name="countries_filtered.csv",
            mime="text/csv"
        )

        bar_chart = alt.Chart(countries_df).mark_bar().encode(
            x=alt.X('name', sort='-y'),
            y='population',
            tooltip=['name', 'population']
        ).properties(width=700, height=400, title="Población por País")
        st.altair_chart(bar_chart, use_container_width=True)

        scatter_chart = alt.Chart(countries_df).mark_circle(size=60).encode(
            x='area',
            y='density',
            color='region',
            tooltip=['name', 'area', 'density']
        ).properties(width=700, height=400, title="Área vs Densidad")
        st.altair_chart(scatter_chart, use_container_width=True)
# --- COVID ---
with tab2:
    query_covid = {}
    if covid_countries:
        query_covid["country"] = {"$in": covid_countries}

    covid_cursor = db_covid.processed_covid.find(query_covid)
    covid_list = list(covid_cursor)
    # Convertir ObjectId a string para evitar error en pyarrow
    for doc in covid_list:
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
    covid_df = pd.DataFrame(covid_list)

    st.markdown("ℹ️ **Los datos COVID se actualizan automáticamente cada 24 horas.**")

    if covid_df.empty:
        st.warning("No hay datos COVID con esos filtros.")
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
                    return f"hace {seconds} segundos"
                minutes = seconds // 60
                if minutes < 60:
                    return f"hace {minutes} minutos"
                hours = minutes // 60
                if hours < 24:
                    return f"hace {hours} horas"
                days = hours // 24
                return f"hace {days} días"

            updated_ago = natural_time(diff)

            st.subheader(f"Datos COVID para {row['country']}")
            col1, col2, col3, col4, col5 = st.columns(5)

            col1.metric("Casos activos", f"{row['active']:,}", help="Personas actualmente infectadas")
            col2.metric("Casos totales", f"{row['cases']:,}", help="Total de casos confirmados")
            col3.metric("Muertes", f"{row['deaths']:,}", help="Total de fallecimientos", delta_color="inverse")
            col4.metric("Recuperados", f"{row['recovered']:,}", help="Personas recuperadas con éxito")
            col5.metric("Letalidad", f"{row['fatality_rate']:.2%}", help="Porcentaje de fallecimientos sobre casos confirmados")

            st.markdown(f"**Última actualización:** {updated_str} ({updated_ago})")

            summary_df = pd.DataFrame({
                "Estado": ["Activos", "Muertes", "Recuperados"],
                "Cantidad": [row['active'], row['deaths'], row['recovered']]
            })

            bar = alt.Chart(summary_df).mark_bar().encode(
                x=alt.X('Estado', sort=None),
                y='Cantidad',
                color=alt.Color('Estado', scale=alt.Scale(domain=["Activos", "Muertes", "Recuperados"], range=["orange", "red", "green"])),
                tooltip=['Estado', 'Cantidad']
            ).properties(width=600, height=300, title="Resumen COVID")

            st.altair_chart(bar, use_container_width=True)

        else:
            st.subheader("KPIs")
            col1, col2, col3 = st.columns(3)
            col1.metric("Total casos", f"{covid_df['cases'].sum():,}")
            col2.metric("Total muertes", f"{covid_df['deaths'].sum():,}")
            col3.metric("Prom. letalidad", f"{covid_df['fatality_rate'].mean():.2%}")

            st.dataframe(covid_df)

            covid_bar = alt.Chart(covid_df).mark_bar().encode(
                x=alt.X('country', sort='-y'),
                y='cases',
                tooltip=['country', 'cases', 'deaths']
            ).properties(width=700, height=400, title="Casos por País")
            st.altair_chart(covid_bar, use_container_width=True)

            recovery_chart = alt.Chart(covid_df).mark_bar(color='green').encode(
                x='country',
                y='recovered',
                tooltip=['country', 'recovered']
            ).properties(width=700, height=400, title="Recuperados por País")
            st.altair_chart(recovery_chart, use_container_width=True)

# --- Clima ---
with tab3:
    date_prefix = date_filter.strftime("%Y-%m-%d")

    weather_doc = db_weather.processed_weather.find_one({
        "summary_time": {"$regex": f"^{date_prefix}"}
    })

    st.markdown("ℹ️ **Los datos de clima se actualizan automáticamente cada 24 horas.**")

    if weather_doc:
        records = weather_doc.get("records", [])
        # Convertir ObjectId a string en records si existe
        for rec in records:
            if "_id" in rec:
                rec["_id"] = str(rec["_id"])
        weather_df = pd.DataFrame(records)

        st.caption(f"Última actualización: {weather_doc.get('summary_time')}")

        weather_df["time"] = pd.to_datetime(weather_df["time"]).dt.tz_localize(None)
        weather_df = weather_df[weather_df["time"].dt.date == date_filter]
        weather_df = weather_df.reset_index(drop=True)


        st.subheader(f"Clima - {date_filter}")

        col1, col2 = st.columns(2)
        col1.metric("Temp. promedio °C", f"{weather_df['temperature_c'].mean():.1f}")
        col2.metric("Máx. lluvia mm", f"{weather_df['rain_mm'].max():.1f}")

        st.dataframe(weather_df)

        temp_line = alt.Chart(weather_df).mark_line().encode(
            x='time',
            y='temperature_c',
            tooltip=['time', 'temperature_c']
        ).properties(width=700, height=400, title="Temperatura durante el día")
        st.altair_chart(temp_line, use_container_width=True)

    else:
        st.warning("No hay datos de clima para la fecha seleccionada.")



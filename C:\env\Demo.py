import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import requests

st.markdown("""
    <style>
    h1 {
        font-size: 32px !important; /* header */
    }
    h2 {
        font-size: 24px !important; /* subheader */
    }
    h3 {
        font-size: 20px !important;
    }
    </style>
""", unsafe_allow_html=True)


st.markdown(
    "<h1 style='text-align: center;'>🎨🏛️ Harvard's Artifacts Collection",
    unsafe_allow_html=True
)

st.header("Data Collection")
# --- User Selection ---
option = st.selectbox(
    "Select a classification:",
    ("Vessels", "Coins", "Drawings", "Paintings", "Fragments"),
)
st.write("You selected:", option)


Collectbutton = st.button("Collect Data", type="primary")

st.header("Migrate to SQL")
Migratebutton = st.button("Insert Data", type="primary")



# --- Define function to fetch data ---
def fetch_artifact_data(classification):
    API_KEY = "b3c786d6-8323-429b-941a-f4f7a6881caf"
    url = "https://api.harvardartmuseums.org/object"
    params = {
        "apikey": API_KEY,
        "size": 100,
        "classification": classification,
        "page": 1
    }

    metadata, media, colors = [], [], []

    try:
        total_records = 0
        while total_records < 2500:  # limit to 2500 total
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            records = data.get("records", [])
            if not records:
                break

            for record in records:
                metadata.append({
                    "id": record.get("id"),
                    "title": record.get("title"),
                    "culture": record.get("culture"),
                    "period": record.get("period"),
                    "century": record.get("century"),
                    "medium": record.get("medium"),
                    "dimensions": record.get("dimensions"),
                    "description": record.get("description"),
                    "department": record.get("department"),
                    "classification": record.get("classification"),
                    "accessionyear": record.get("accessionyear"),
                    "accessionmethod": record.get("accessionmethod"),
                })

                media.append({
                    "objectid": record.get("objectid"),
                    "imagecount": record.get("imagecount"),
                    "mediacount": record.get("mediacount"),
                    "colorcount": record.get("colorcount"),
                    "rank": record.get("rank"),
                    "datebegin": record.get("datebegin"),
                    "dateend": record.get("dateend"),
                })

                for c in record.get("colors", []):
                    colors.append({
                        "objectid": record.get("objectid"),
                        "color": c.get("color"),
                        "spectrum": c.get("spectrum", ""),
                        "hue": c.get("hue", ""),
                        "percent": c.get("percent", ""),
                        "css3": c.get("css3", "")
                    })

            total_records += len(records)
            if "info" in data and data["info"].get("next"):
                params["page"] += 1
            else:
                break

        st.success(f"✅ Retrieved {total_records} records successfully!")

    except requests.exceptions.RequestException as e:
        st.error(f"❌ Failed to fetch data: {e}")
        return [], [], []

    return metadata, media, colors


# --- Session state to persist data between buttons ---
if "metadata" not in st.session_state:
    st.session_state.metadata = []
if "media" not in st.session_state:
    st.session_state.media = []
if "colors" not in st.session_state:
    st.session_state.colors = []


# --- Fetch Data ---
if Collectbutton:
    with st.spinner("Collecting data from Harvard API..."):
        metadata, media, colors = fetch_artifact_data(option)
        st.session_state.metadata = metadata
        st.session_state.media = media
        st.session_state.colors = colors

    if metadata:
        st.subheader("🧾 Metadata")
        st.dataframe(pd.DataFrame(metadata))

        st.subheader("🖼 Media")
        st.dataframe(pd.DataFrame(media))

        st.subheader("🎨 Colors")
        st.dataframe(pd.DataFrame(colors))
    else:
        st.warning("No records found for the selected classification.")


# --- Migrate Data to SQLite ---
if Migratebutton:
    if not st.session_state.metadata:
        st.warning("⚠️ Please collect data first before migrating to SQL.")
    else:
        db_path = r'C:\Users\Sundari\OneDrive\Desktop\GUVI DOCS\SQL\Harvard2.db'
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")

            # Create tables if they don't exist
            cursor.execute('''CREATE TABLE IF NOT EXISTS artifact_metadata (
                id INTEGER PRIMARY KEY,
                title TEXT,
                culture TEXT,
                period TEXT,
                century TEXT,
                medium TEXT,
                dimensions TEXT,
                description TEXT,
                department TEXT,
                classification TEXT, 
                accessionyear INTEGER,
                accessionmethod TEXT
            )''')

            cursor.execute('''CREATE TABLE IF NOT EXISTS artifact_media (
                objectid INTEGER PRIMARY KEY,
                imagecount INTEGER,
                mediacount INTEGER,
                colorcount INTEGER,
                rank INTEGER,
                datebegin INTEGER,
                dateend INTEGER,
                FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
            )''')

            cursor.execute('''CREATE TABLE IF NOT EXISTS artifact_colors (
                objectid INTEGER,
                color TEXT,
                spectrum TEXT,
                hue TEXT,
                percent TEXT,
                css3 TEXT,
                UNIQUE(objectid, color)          
            );
            ''')

            # Insert data
            upsert_metadata_sql = '''
            INSERT INTO artifact_metadata (
              id, title, culture, period, century,
              medium, dimensions, description, department,
              classification, accessionyear, accessionmethod
            ) VALUES (
              :id, :title, :culture, :period, :century,
              :medium, :dimensions, :description, :department,
              :classification, :accessionyear, :accessionmethod
            )
            ON CONFLICT(id) DO UPDATE SET
              title = excluded.title,
              culture = excluded.culture,
              period = excluded.period,
              century = excluded.century,
              medium = excluded.medium,
              dimensions = excluded.dimensions,
              description = excluded.description,
              department = excluded.department,
              classification = excluded.classification,
              accessionyear = excluded.accessionyear,
              accessionmethod = excluded.accessionmethod;
            '''
            upsert_media_sql = '''
            INSERT INTO artifact_media (
              objectid, imagecount, mediacount,
              colorcount, rank, datebegin, dateend
            ) VALUES (
              :objectid, :imagecount, :mediacount,
              :colorcount, :rank, :datebegin, :dateend
            )
            ON CONFLICT(objectid) DO UPDATE SET
              imagecount = excluded.imagecount,
              mediacount = excluded.mediacount,
              colorcount = excluded.colorcount,
              rank = excluded.rank,
              datebegin = excluded.datebegin,
              dateend = excluded.dateend;
            '''
            upsert_colors_sql = '''
            INSERT INTO artifact_colors (
              objectid, color, spectrum, hue, percent, css3
            ) VALUES (
              :objectid, :color, :spectrum, :hue, :percent, :css3
            )
            ON CONFLICT(objectid, color) DO UPDATE SET
              spectrum = excluded.spectrum,
              hue = excluded.hue,
              percent = excluded.percent,
              css3 = excluded.css3;
            '''

            # Insert / Upsert
            cursor.executemany(upsert_metadata_sql, st.session_state.metadata)
            cursor.executemany(upsert_media_sql, st.session_state.media)
            cursor.executemany(upsert_colors_sql, st.session_state.colors)

            conn.commit()
            st.success("✅ Data successfully migrated to SQLite database!")

            df1 = pd.read_sql_query("SELECT * FROM artifact_metadata", conn)
            st.subheader("🧾 Artifact Metadata Table")
            st.dataframe(df1)
            df2 = pd.read_sql_query("SELECT * FROM artifact_media", conn)
            st.subheader("🖼 Artifact Media Table")
            st.dataframe(df2)
            df3 = pd.read_sql_query("SELECT * FROM artifact_colors", conn)
            st.subheader("🎨 Artifact Colors Table")
            st.dataframe(df3)

st.header("SQL Query")
# SQL Querries

queries = {
    "1.List all artifacts from the 11th century belonging to Byzantine culture": 
        "SELECT * FROM artifact_metadata WHERE century = '11th century' AND culture = 'Byzantine'",
    "2.What are the unique cultures represented in the artifacts?": 
        "SELECT culture FROM artifact_metadata GROUP BY culture",
    "3.List all artifacts from the Archaic Period.": 
        "SELECT * FROM artifact_metadata WHERE period = 'Archaic period'",
    "4.List artifact titles ordered by accession year (newest first)": 
        "SELECT title, accessionyear FROM artifact_metadata WHERE accessionyear IS NOT NULL ORDER BY accessionyear DESC",
    "5.How many artifacts are there per department?": 
        "SELECT department, COUNT(*) AS artifact_count FROM artifact_metadata GROUP BY department ORDER BY artifact_count DESC",
    "6.Which artifacts have more than 1 image?":
        "select * from artifact_media where imagecount>1",
    "7.What is the average rank of all artifacts?":
        "SELECT AVG(rank) AS average_rank FROM artifact_media WHERE rank IS NOT NULL;",
    "8.Which artifacts have a higher colorcount than mediacount?":
        "select * from artifact_media where colorcount>mediacount;",
    "9.List all artifacts created between 1500 and 1600.":
        "select * from artifact_media where datebegin>1500 AND dateend<=1600",
    "10.How many artifacts have no media files?":
        "select * from artifact_media WHERE mediacount = 0 OR mediacount IS NULL;",
    "11.What are all the distinct hues used in the dataset?":
        "select distinct hue from artifact_colors",
    "12.What are the top 5 most used colors by frequency?":
        "SELECT color, count(color) AS Frequency FROM artifact_colors group by color ORDER by Frequency DESC limit 5;",
    "13.What is the average coverage percentage for each hue?":
        "select hue AS Hue, AVG(percent) AS average_percent FROM artifact_colors group by hue",
    "14.List all colors used for a given artifact ID.":
        "SELECT objectid,GROUP_CONCAT(DISTINCT color) AS colors FROM artifact_colors WHERE color IS NOT NULL GROUP BY objectid;",
    "15.What is the total number of color entries in the dataset?":
        "SELECT COUNT(*) AS total_color_entries FROM artifact_colors;",
    "16.List artifact titles and hues for all artifacts belonging to the Byzantine culture.":
        "SELECT md.title,ac.hue,md.culture FROM artifact_metadata AS md JOIN artifact_colors AS ac ON md.id = ac.objectid WHERE md.culture = 'Byzantine'",
    "17.List each artifact title with its associated hues.":
        "SELECT md.title,ac.hue FROM artifact_metadata AS md JOIN artifact_colors AS ac ON md.id = ac.objectid",
    "18.Get artifact titles, cultures, and media ranks where the period is not null.":
        "Select md.title, md.culture, mm.rank, md.period from artifact_metadata AS md JOIN artifact_media AS mm on md.id = mm.objectid where md.period is NOT NULL",
    "19.Find artifact titles ranked in the top 10 that include the color hue 'Grey'.":
        "SELECT md.title,am.rank,GROUP_CONCAT(DISTINCT ac.hue) AS hues FROM artifact_metadata AS md JOIN artifact_media AS am ON md.id = am.objectid JOIN artifact_colors AS ac ON md.id = ac.objectid WHERE ac.hue = 'Grey' GROUP BY md.title, am.rank ORDER BY am.rank DESC LIMIT 10;",
    "20.How many artifacts exist per classification, and what is the average media count for each?":
        "SELECT md.classification, COUNT(md.id) AS artifact_count, AVG(am.mediacount) AS avg_media_count FROM artifact_metadata AS md JOIN  artifact_media AS am ON md.id = am.objectid WHERE md.classification IS NOT NULL GROUP BY md.classification",
    "21.List all artifacts from the 1st-2nd century CE belonging to Chinese culture": 
        "SELECT * FROM artifact_metadata WHERE century = '1st-2nd century CE' AND culture = 'Chinese'",
    "22.List all artifacts with imagecount>=30":
        "SELECT * from artifact_media WHERE imagecount>=30",
    "23.Count how many color records exist per artifact":
		"SELECT objectid, COUNT(*) AS color_count FROM artifact_colors GROUP BY objectid",
    "24.List artifacts that have no images":
		"SELECT md.id, md.title FROM artifact_metadata AS md JOIN artifact_media AS mm ON md.id = mm.objectid WHERE mm.imagecount = 0 OR mm.imagecount IS NULL;",
    "25.Count total number of artifacts in the database":
		"SELECT COUNT(*) AS total_artifacts FROM artifact_metadata;"
}

query_labels = ["-- Select a query --"] + list(queries.keys())
selected_query_label = st.selectbox("Choose a query:", query_labels, index=0)

# --- Ensure session state key exists ---
if "query_result" not in st.session_state:
    st.session_state.query_result = None

# --- Clear previous results if default selected ---
if selected_query_label == "-- Select a query --" and st.session_state.query_result is not None:
    st.session_state.query_result = None
    st.info("Please select a query to run.")

# --- Run Query button (always enabled) ---
run_query_button = st.button("Run Query")

# --- Execute query ---
if run_query_button:
    if selected_query_label == "-- Select a query --":
        st.warning("⚠️ Please select a valid query from the dropdown.")
    else:
        selected_query = queries[selected_query_label]
        db_path = r'C:\Users\Sundari\OneDrive\Desktop\GUVI DOCS\SQL\Harvard2.db'
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(selected_query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            st.session_state.query_result = pd.DataFrame(rows, columns=columns)
        st.success("✅ Query executed successfully!")

# --- Display results ---
if st.session_state.query_result is not None:
    st.subheader("Query Results")
    st.dataframe(st.session_state.query_result)

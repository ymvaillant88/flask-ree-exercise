import io
import os
import matplotlib
from flask import Flask, request, jsonify, send_file
import requests as requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import plotly.express as px
from flask_cors import CORS  # Para permitir peticiones desde cualquier origen

matplotlib.pyplot.switch_backend('Agg')

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})


def string_to_timestamp(string):
    return datetime.strptime(string, "%Y-%m-%dT%H:%M")


def string_to_timestamp2(fecha_str):
    fecha_str_sin_milisegundos = fecha_str.split(".")[0]  # Eliminamos los milisegundos
    fecha_str_sin_zona_horaria = fecha_str_sin_milisegundos.split("+")[
        0]  # Eliminamos la información de la zona horaria
    return datetime.strptime(fecha_str_sin_zona_horaria, "%Y-%m-%dT%H:%M:%S")


def validate_form(start_date, end_date, orientation, chart_type):
    try:
        # orientation v o h
        if orientation != "v" and orientation != "h":
            return False, "La orientación debe ser v o h"
        # chart_type line o bar
        if chart_type != "line" and chart_type != "bar":
            return False, "El tipo de gráfico debe ser line o bar"

        start = datetime.strptime(start_date, "%Y-%m-%dT%H:%M")
        end = datetime.strptime(end_date, "%Y-%m-%dT%H:%M")
        min_date = datetime.strptime("2020-01-01T00:00", "%Y-%m-%dT%H:%M")
        max_date = datetime.strptime("2022-12-31T23:59", "%Y-%m-%dT%H:%M")
        if start < min_date or end > max_date:
            return False, "El rango de fechas debe estar entre 2020-01-01T00:00 y 2022-12-31T23:59"
        else:
            diff = end - start
            cantidad_horas = diff.total_seconds() / 3600
            if cantidad_horas > 744:
                return False, "El rango de fechas no puede ser mayor a 31 días"
            else:
                return True, "OK"
    except ValueError:
        return False, "El formato de las fechas no es correcto: YYYY-MM-DDTHH:MM"


def get_chart(chart_type, orientation, df_BD):
    if chart_type == 'line':
        if orientation == 'v':
            fig = px.line(df_BD, x='datetime', y='demand', orientation='v')
            fig.update_layout(
                title="Demanda de energía eléctrica en España",
                xaxis_title="Fecha",
                yaxis_title="Demanda (MW)",
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                )
            )
            fig.show()
        elif orientation == 'h':
            fig = px.line(df_BD, x='demand', y='datetime', orientation='h')
            fig.update_layout(
                title="Demanda de energía eléctrica en España",
                xaxis_title="Demanda (MW)",
                yaxis_title="Fecha",
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                )
            )
            fig.show()
        else:
            return "La orientación debe ser vertical u horizontal"
    elif chart_type == 'bar':
        if orientation == 'v':
            fig = px.bar(df_BD, x='datetime', y='demand', orientation='v')
            fig.update_layout(
                title="Demanda de energía eléctrica en España",
                xaxis_title="Fecha",
                yaxis_title="Demanda (MW)",
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                )
            )
            fig.show()
        elif orientation == 'h':
            fig = px.bar(df_BD, x='demand', y='datetime', orientation='h')
            fig.update_layout(
                title="Demanda de energía eléctrica en España",
                xaxis_title="Demanda (MW)",
                yaxis_title="Fecha",
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                )
            )
            fig.show()
        else:
            return "La orientación debe ser vertical u horizontal"
    else:
        return "El tipo de gráfico debe ser line o bar"
        # Guardar el gráfico en un objeto BytesIO
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)

    # Devolver el gráfico como respuesta
    return send_file(buffer, mimetype='image/png')


@app.route('/', methods=['GET'])
def index():
    return "API de datos de demanda de energía eléctrica en España"


# ENDPOINT 1
@app.route('/get_demand', methods=['GET'])
def get_demand():
    if "start_date" in request.args and "end_date" in request.args and "orientation" in request.args and "chart_type" in request.args:
        start_date = request.args['start_date']
        end_date = request.args['end_date']
        orientation = request.args['orientation']
        chart_type = request.args['chart_type']

        formatOK, msg = validate_form(start_date, end_date, orientation, chart_type)
        if formatOK:
            url = f"https://apidatos.ree.es/en/datos/demanda/evolucion?start_date={start_date}&end_date={end_date}&time_trunc=hour&geo_trunc=electric_system&geo_limit=peninsular&geo_ids=8741"
            response = requests.get(url)
            if response.status_code != 200:
                return jsonify({'error': 'Error en la respuesta de la API'}), 400
            else:
                data = response.json()
                include = data.get('included')
                attributes = include[0].get('attributes')
                values = attributes.get('values')
                demand = [(string_to_timestamp2(value.get('datetime')), value.get('value')) for value in
                          values]
                df = pd.DataFrame(demand, columns=['datetime', 'demand'])

                if df.shape[0] > 0:
                    engine = create_engine(
                        'postgresql://postgres:W6YM6XkDKZbvG9QLMEJ6@containers-us-west-10.railway.app:5761/railway')

                    if engine:
                        query = f"SELECT * FROM demand WHERE datetime >= '{string_to_timestamp(start_date)}' AND datetime <= '{string_to_timestamp(end_date)}'"
                        print(query)
                        df_BD = pd.read_sql(query, engine)

                        if df_BD.shape[0] > 0:
                            df_new = df[~df['datetime'].isin(df_BD['datetime'])]

                            if df_new.shape[0] > 0:
                                df_new.to_sql('demand', engine, if_exists='append', index=False)
                                engine.dispose()
                                return get_chart(chart_type, orientation, df_new)
                            else:
                                return get_chart(chart_type, orientation, df_BD)
                        else:
                            df.to_sql('demand', engine, if_exists='append', index=False)
                            engine.dispose()
                            return get_chart(chart_type, orientation, df)

                    else:
                        return f"No se pudo establecer la conexión con la base de datos, error: {engine}"

                else:
                    return "No hay datos de demanda en la Red Eléctrica de España para el rango de fechas dado"
        else:
            return msg  # msg: mensaje de error de la función validate_form (línea 11)
    else:
        return "Faltan parámetros en la consulta: debe de incluir start_date, end_date, orientation y chart_type"


# ENDPOINT 2:
@app.route('/get_db_data', methods=['GET'])
def get_db_data():
    if "start_date" in request.args and "end_date" in request.args:
        start_date = request.args['start_date']
        end_date = request.args['end_date']
        try:
            start = datetime.strptime(start_date, "%Y-%m-%dT%H:%M")
            end = datetime.strptime(end_date, "%Y-%m-%dT%H:%M")
            min_date = datetime.strptime("2020-01-01T00:00", "%Y-%m-%dT%H:%M")
            max_date = datetime.strptime("2022-12-31T23:59", "%Y-%m-%dT%H:%M")
            if start < min_date or end > max_date:
                return "El rango de fechas debe estar entre 2020-01-01T00:00 y 2022-12-31T23:59"
            else:
                diff = end - start
                cantidad_horas = diff.total_seconds() / 3600
                if cantidad_horas > 744:
                    return "El rango de fechas no puede ser mayor a 31 días"
                else:
                    formatOK = True
        except ValueError:
            formatOK = False

        if formatOK:
            engine = create_engine(
                'postgresql://postgres:W6YM6XkDKZbvG9QLMEJ6@containers-us-west-10.railway.app:5761/railway')

            if engine:
                query = f"SELECT * FROM demand WHERE datetime >= '{string_to_timestamp(start_date)}' and datetime <= '{string_to_timestamp(end_date)}'"
                print(query)
                df_BD = pd.read_sql(query, engine)

                if df_BD.shape[0] > 0:
                    engine.dispose()
                    return df_BD.to_json(orient="records")
                else:
                    query1 = f"SELECT * FROM demand"
                    df_BD1 = pd.read_sql(query1, engine)
                    engine.dispose()
                    return df_BD1.to_json(orient="records")

            else:
                return f"No se pudo establecer la conexión con la base de datos, error: {engine}"
        else:
            return "El formato de las fechas no es correcto: YYY-MM-DDTHH:MM"
    else:
        return "No se han proporcionado las fechas de inicio y fin"


# ENDPOINT 3:
@app.route('/wipe_data', methods=['DELETE'])
def wipe_data():
    if 'secret' in request.args:
        secret = request.args['secret']
        if secret == '1234':
            engine = create_engine(
                'postgresql://postgres:W6YM6XkDKZbvG9QLMEJ6@containers-us-west-10.railway.app:5761/railway')
            # Crear una conexión
            with engine.connect() as connection:
                query = "DELETE FROM demand"
                connection.execute(query)
                engine.dispose()
                return "Datos borrados correctamente"
        else:
            return "No tienes permisos para borrar los datos"
    else:
        return "No se ha proporcionado la contraseña: Argumento secret"


if __name__ == '__main__':
    app.run(debug=True, port=os.getenv("PORT", default=5000))

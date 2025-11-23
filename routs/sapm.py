from flask import Flask, Blueprint, render_template, request, flash
from time import sleep
import pandas as pd
import os

app = Flask(__name__)
app.secret_key = 'supersecretkey'

sapm_bp = Blueprint('sapm', __name__, template_folder='templates')

@sapm_bp.route('/SAPM')
@sapm_bp.route('/SAPM.html')
def sapm():
    return render_template('SAPM.html')

@sapm_bp.route('/processar_csv', methods=['POST'])
def processar_csv():
    nomes_esperados = [
        "Objetos Situação - CEINT_CTA.csv",
        "Objetos Situação - CEINT_RJ.csv",
        "Objetos Situação - CEINT_SP.csv",
        "Objetos Situação - CEINT_VAL.csv"
    ]

    arquivos_recebidos = request.files.getlist('arquivo_csv')
    nomes_recebidos = [arquivo.filename for arquivo in arquivos_recebidos]

    # Verifica duplicidade
    if len(nomes_recebidos) != len(set(nomes_recebidos)):
        flash('Arquivos duplicados detectados. Envie apenas um de cada tipo.')
        return render_template('sapm.html')

    # Verifica se todos os arquivos esperados foram enviados
    faltando = [nome for nome in nomes_esperados if nome not in nomes_recebidos]
    if faltando:
        flash(f"Arquivos faltando: {', '.join(faltando)}")
        return render_template('sapm.html')

    os.makedirs('uploads', exist_ok=True)

    try:
        dfs = []
        for arquivo in arquivos_recebidos:
            caminho = os.path.join('uploads', arquivo.filename)
            arquivo.save(caminho)
            df = pd.read_csv(caminho, delimiter=';', encoding='latin1')
            dfs.append(df)

        df = pd.concat(dfs, ignore_index=True)

        df.columns = df.columns.str.encode('latin1').str.decode('utf-8')
        df.rename(columns={
            'ESTAÇÃO': 'ESTACAO',
            'TOIR': 'QTDE TOTAL OBJETOS',
            'IPCI': 'PRODUTIVIDADE_%'
        }, inplace=True)

        ceint_mapping = {
            '10.150': 'CEINT_CTA',
            '10.159': 'CEINT_RJO',
            '10.192': 'CEINT_SPO',
            '10.204': 'CEINT_VAL'
        }

        def get_ceint(ip):
            prefix = '.'.join(ip.split('.')[:2])
            return ceint_mapping.get(prefix, 'UNKNOWN')

        df['CEINT'] = df['ESTACAO'].apply(get_ceint)
        df = df[df['CEINT'] != 'UNKNOWN']

        df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
        df = df.dropna(subset=['DATA'])
        df['DIA'] = df['DATA'].dt.date
        df['MINUTO'] = df['DATA'].dt.floor('min')
        df['THEU'] = df.groupby(['CEINT', 'ESTACAO', 'DIA'])['MINUTO'].transform(lambda x: x.nunique())
        df['QTDE TOTAL OBJETOS'] = df.groupby(['CEINT', 'ESTACAO', 'DIA'])['ETIQUETA'].transform('count')
        df = df.drop_duplicates(subset=['CEINT', 'ESTACAO', 'DIA'])

        df['PRODUTIVIDADE_%'] = df.apply(
            lambda row: (row['QTDE TOTAL OBJETOS'] / (row['THEU'] / 60 * 812)) * 100 if row['THEU'] > 0 else 0,
            axis=1
        )
        df['PRODUTIVIDADE_%'] = df['PRODUTIVIDADE_%'].apply(lambda x: f"{x:.2f}")

        nova_media_ponderada_por_ceint = (
            df
            .groupby('CEINT')
            .apply(lambda g: (g['QTDE TOTAL OBJETOS'].sum() / (g['THEU'].sum() / 60 * 812)) * 100 if g['THEU'].sum() > 0 else 0)
        )

        df['MÉDIA PONDERADA'] = ''
        for ceint, media in nova_media_ponderada_por_ceint.items():
            idx = df[df['CEINT'] == ceint].index.min()
            df.at[idx, 'MÉDIA PONDERADA'] = f"{media:.2f}"

        df['DIA'] = df['DIA'].apply(lambda x: x.strftime('%d/%m/%Y'))

        colunas_para_remover = ['DATA', 'SERVIÇO', 'CEP DESTINO', 'PRC?', 'UNITIZADOR', 'USUÁRIO', 'NOME USUÁRIO', 'MINUTO']
        df.drop(columns=[col for col in colunas_para_remover if col in df.columns], inplace=True)

        df['QTDE OBJETOS X MINUTOS'] = df.apply(
            lambda row: f"{row['QTDE TOTAL OBJETOS'] / row['THEU']:.2f}" if row['THEU'] > 0 else "0.00",
            axis=1
        )

        with pd.ExcelWriter('Objetos Situação.xlsx', engine='openpyxl') as writer:
            for ceint in ceint_mapping.values():
                df_ceint = df[df['CEINT'] == ceint].copy()
                if not df_ceint.empty:
                    df_ceint.to_excel(writer, sheet_name=ceint, index=False)

        flash("Arquivo 'Objetos Situação.xlsx' gerado com sucesso com a nova média ponderada.")
    except Exception as e:
        flash(f"Erro ao processar o arquivo: {str(e)}")

    sleep(2)
    return render_template('SAPM.html')

app.register_blueprint(sapm_bp)

if __name__ == '__main__':
    app.run(debug=True)





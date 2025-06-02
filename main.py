import pandas as pd
import unicodedata
import re
import dbf


def remover_acentos(texto) -> str:
    texto = ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )
    texto = re.sub(r'[^a-zA-Z0-9\s]', '', texto)
    texto = texto.replace(u'\xa0', u'')
    return texto

def limitar_campo_descricao(texto:str) -> str:
    texto = texto[0:150]
    return texto

def check_colunas(dataframe: pd.DataFrame):
    colunas = []
    
    if 'CODIGO' not in dataframe.columns:
        colunas.append('CODIGO')

    if 'DESCRICAO' not in dataframe.columns:
        colunas.append('DESCRICAO')

    if 'VL_PROCEDI' not in dataframe.columns:
        colunas.append('VL_PROCEDI')

    if 'VL_CUSTO_O' not in dataframe.columns:
        colunas.append('VL_CUSTO_O')

    if 'VL_ANESTES' not in dataframe.columns:
        colunas.append('VL_ANESTES')
    
    if 'VL_MEDICO' not in dataframe.columns:
        colunas.append('VL_MEDICO')
    
    if 'VL_FILME' not in dataframe.columns:
        colunas.append('VL_FILME')
    
    if 'QT_FILME' not in dataframe.columns:
        colunas.append('QT_FILME')
    
    if 'VL_AUXILIA' not in dataframe.columns:
        colunas.append('VL_AUXILIA')
    
    if 'NR_AUX' not in dataframe.columns:
        colunas.append('NR_AUX')

    if 'PORTE_ANES' not in dataframe.columns:
        colunas.append('PORTE_ANES')

    if len(colunas) > 0:
        cols = ', '.join(colunas)
        print(f'A colunas "{cols}" não foram encontradas no layout!')
        input('Pressione ENTER para sair do programa.')
        exit()



def to_dbf( dataframe: pd.DataFrame ):
    df = dataframe
    headers = """
        CODIGO N(8,0); 
        DESCRICAO C(150); 
        VL_PROCEDI N(10,4); 
        VL_CUSTO_O C(10); 
        VL_ANESTES C(10); 
        VL_MEDICO C(10); 
        VL_FILME C(10);
        QT_FILME C(10);
        VL_AUXILIA C(10);
        NR_AUX C(3);
        PORTE_ANES C(3);
    """
    
    tb = dbf.Table(r'carga.dbf',headers)
    tb.open(mode=dbf.READ_WRITE)
    
    for row in df.itertuples(index=False):
        tb.append((
            row.CODIGO,
            row.DESCRICAO,
            row.VL_PROCEDI,
            row.VL_CUSTO_O,
            row.VL_ANESTES,
            row.VL_MEDICO,
            row.VL_FILME,
            row.QT_FILME,
            row.VL_AUXILIA,
            row.NR_AUX,
            row.PORTE_ANES,
        ))
        
    tb.close()

def main():
    print("Conversão iniciada")
    df = pd.read_excel(r'carga.xlsx')
    check_colunas(df)    


    df['DESCRICAO'] = df['DESCRICAO'].apply(remover_acentos)
    df['DESCRICAO'] = df['DESCRICAO'].apply(limitar_campo_descricao)
    
    df['VL_CUSTO_O'] = df['VL_CUSTO_O'].where(pd.notnull(df['VL_CUSTO_O']), '').astype(str)
    df['VL_ANESTES'] = df['VL_ANESTES'].where(pd.notnull(df['VL_ANESTES']), '').astype(str)
    df['VL_MEDICO'] = df['VL_MEDICO'].where(pd.notnull(df['VL_MEDICO']), '').astype(str)
    df['VL_FILME'] = df['VL_FILME'].where(pd.notnull(df['VL_FILME']), '').astype(str)
    df['QT_FILME'] = df['QT_FILME'].where(pd.notnull(df['QT_FILME']), '').astype(str)
    df['VL_AUXILIA'] = df['VL_AUXILIA'].where(pd.notnull(df['VL_AUXILIA']), '').astype(str)
    df['NR_AUX'] = df['NR_AUX'].where(pd.notnull(df['NR_AUX']), '').astype(str)
    df['PORTE_ANES'] = df['PORTE_ANES'].where(pd.notnull(df['PORTE_ANES']), '').astype(str)

    to_dbf(df)
    print("Conversão finalizada")


if __name__ == '__main__':
    main()
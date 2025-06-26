# Script para web scraping e extração de dados de substâncias controladas da ANVISA
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time

# --- Funções Auxiliares ---
def get_soup(url):
    """Busca e parseia o conteúdo HTML de uma URL."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = response.apparent_encoding if response.apparent_encoding else 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar URL {url}: {e}")
        return None

def extract_text_from_anvisa_page(soup):
    """Extrai o texto principal de uma página do anvisalegis."""
    if not soup:
        return ""
    content_div = soup.find('div', id='texto-dou') or \
                  soup.find('div', id='texto-norma') or \
                  soup.find('div', class_='WordSection1') or \
                  soup.find('div', class_='texto-dou')

    if content_div:
        return content_div.get_text(separator='\n', strip=True)
    else:
        body_text = soup.body.get_text(separator='\n', strip=True)
        if body_text and len(body_text) > 500:
             print("Aviso: Div de conteúdo principal não encontrada, usando texto do body como fallback.")
             return body_text
        print("Aviso: Não foi possível extrair o texto principal da norma de forma estruturada.")
        return ""


def parse_substances_from_text(text, current_list_name, action_type, mes_ano_evento):
    """
    Extrai substâncias de um bloco de texto (INCLUSÃO ou EXCLUSÃO).
    Retorna uma lista de dicionários.
    """
    substances = []
    items = re.findall(r'(?:\d+\.\s*|\-\s*|\•\s*)(.*?)(?=\n\s*(?:\d+\.|\-|\•)|$)', text, re.IGNORECASE | re.DOTALL)

    if not items:
        raw_lines = text.split('\n')
        items = [line.strip() for line in raw_lines if line.strip() and not line.strip().lower().startswith(("lista", "anexo", "art.", "parágrafo", "inciso", "capítulo"))]

    for item in items:
        principio_ativo = item.strip()
        principio_ativo = re.sub(r'^\d+\.\s*|^[\-\•]\s*', '', principio_ativo).strip()
        principio_ativo = re.split(r'\s*ADENDO[\s\:]|\s*OBS[\.:]|\s*\(\s*ver\s+nota|\s*NOTA[\s\:]', principio_ativo, 1, re.IGNORECASE)[0].strip()

        if not principio_ativo or len(principio_ativo) < 3:
            continue

        record = {
            'principio_ativo': principio_ativo.upper(),
            'lista': current_list_name,
            'mes_ano_inclusao': None,
            'mes_ano_exclusao': None
        }
        # Corrigido para usar 'INCLUSÃO' e 'EXCLUSÃO' consistentemente
        if action_type == 'INCLUSÃO':
            record['mes_ano_inclusao'] = mes_ano_evento
        elif action_type == 'EXCLUSÃO':
            record['mes_ano_exclusao'] = mes_ano_evento
        substances.append(record)
    return substances

# --- Processamento Principal ---
medicamentos_controlados = []

# 1. Processar Portaria SVS/MS nº 344/1998 (Texto Original)
print("Processando Portaria SVS/MS nº 344/1998...")
url_p344_1998 = "https://anvisalegis.datalegis.net/action/UrlPublicasAction.php?acao=abrirAtoPublico&num_ato=00000344&sgl_tipo=POR&sgl_orgao=SVS/MS&vlr_ano=1998&seq_ato=000&cod_modulo=134&cod_menu=1696"
soup_p344 = get_soup(url_p344_1998)
time.sleep(2)

if soup_p344:
    text_p344 = extract_text_from_anvisa_page(soup_p344)
    mes_ano_p344 = "05/1998"

    anexo_i_match = re.search(r'ANEXO I\s*\n\s*LISTAS DE SUBSTÂNCIAS.*?SUBMETIDAS A CONTROLE ESPECIAL(.*?)(?:ANEXO II|ANEXO III|\Z)', text_p344, re.IGNORECASE | re.DOTALL)

    if anexo_i_match:
        anexo_i_text = anexo_i_match.group(1)
        list_patterns = {
            'A1': r'LISTA\s*["\']A1["\']\s*LISTA DAS SUBSTÂNCIAS ENTORPECENTES(.*?)(?=LISTA\s*["\']A2["\']|LISTA\s*["\']B1["\']|\Z)',
            'A2': r'LISTA\s*["\']A2["\']\s*LISTA DAS SUBSTÂNCIAS ENTORPECENTES DE USO PERMITIDO SOMENTE EM CONCENTRAÇÕES ESPECIAIS(.*?)(?=LISTA\s*["\']A3["\']|LISTA\s*["\']B1["\']|\Z)',
            'A3': r'LISTA\s*["\']A3["\']\s*LISTA DAS SUBSTÂNCIAS PSICOTRÓPICAS(.*?)(?=LISTA\s*["\']B1["\']|\Z)',
            'B1': r'LISTA\s*["\']B1["\']\s*LISTA DAS SUBSTÂNCIAS PSICOTRÓPICAS(.*?)(?=LISTA\s*["\']B2["\']|\Z)',
            'B2': r'LISTA\s*["\']B2["\']\s*LISTA DAS SUBSTÂNCIAS PSICOTRÓPICAS ANOREXÍGENAS(.*?)(?=LISTA\s*["\']C1["\']|\Z)',
            'C1': r'LISTA\s*["\']C1["\']\s*LISTA DAS OUTRAS SUBSTÂNCIAS SUJEITAS A CONTROLE ESPECIAL(.*?)(?=LISTA\s*["\']C2["\']|\Z)',
            'C2': r'LISTA\s*["\']C2["\']\s*LISTA DE SUBSTÂNCIAS RETINÓICAS(.*?)(?=LISTA\s*["\']C3["\']|\Z)',
            'C3': r'LISTA\s*["\']C3["\']\s*LISTA DE SUBSTÂNCIAS IMUNOSSUPRESSORAS(.*?)(?=LISTA\s*["\']C4["\']|LISTA\s*["\']C5["\']|LISTA\s*["\']D1["\']|\Z)',
            'C5': r'LISTA\s*["\']C5["\']\s*LISTA DAS SUBSTÂNCIAS ANABOLIZANTES(.*?)(?=LISTA\s*["\']D1["\']|ADENDO|\Z)',
        }

        for lista_nome, pattern in list_patterns.items():
            match = re.search(pattern, anexo_i_text, re.IGNORECASE | re.DOTALL)
            if match:
                print(f"  Encontrada {lista_nome} na P344/98.")
                substances_text = match.group(1)
                # Corrigido para 'INCLUSÃO'
                parsed_substances = parse_substances_from_text(substances_text, lista_nome, 'INCLUSÃO', mes_ano_p344)
                medicamentos_controlados.extend(parsed_substances)
            else:
                print(f"  {lista_nome} não encontrada no formato esperado na P344/98.")
        if any(p['principio_ativo'] == 'TALIDOMIDA' for p in medicamentos_controlados if p['lista'] == 'C3'):
             print("   TALIDOMIDA (C3) processada.")
        else:
            if "TALIDOMIDA" in anexo_i_text.upper() and "LISTA \"C3\"" in anexo_i_text:
                print("   Adicionando TALIDOMIDA (C3) manualmente com base na P344/98.")
                medicamentos_controlados.append({'principio_ativo': 'TALIDOMIDA', 'lista': 'C3', 'mes_ano_inclusao': mes_ano_p344, 'mes_ano_exclusao': None})

# Corrigido nome da variável de dcs_info para rdcs_info
rdcs_info = [
    {"url": "https://anvisalegis.datalegis.net/action/UrlPublicasAction.php?acao=abrirAtoPublico&num_ato=00000337&sgl_tipo=RDC&sgl_orgao=RDC/DC/ANVISA/MS&vlr_ano=2020&seq_ato=000&cod_modulo=310&cod_menu=8542", "mes_ano": "01/2020", "nome": "RDC 337/2020"},
    {"url": "https://anvisalegis.datalegis.net/action/UrlPublicasAction.php?acao=abrirAtoPublico&num_ato=00000345&sgl_tipo=RDC&sgl_orgao=RDC/DC/ANVISA/MS&vlr_ano=2020&seq_ato=000&cod_modulo=310&cod_menu=8542", "mes_ano": "03/2020", "nome": "RDC 345/2020"},
    {"url": "https://anvisalegis.datalegis.net/action/UrlPublicasAction.php?acao=abrirAtoPublico&num_ato=00000368&sgl_tipo=RDC&sgl_orgao=RDC/DC/ANVISA/MS&vlr_ano=2020&seq_ato=000&cod_modulo=310&cod_menu=8542", "mes_ano": "04/2020", "nome": "RDC 368/2020"},
    {"url": "https://anvisalegis.datalegis.net/action/UrlPublicasAction.php?acao=abrirAtoPublico&num_ato=00000404&sgl_tipo=RDC&sgl_orgao=RDC/DC/ANVISA/MS&vlr_ano=2020&seq_ato=000&cod_modulo=310&cod_menu=8542", "mes_ano": "07/2020", "nome": "RDC 404/2020"},
]

for rdc in rdcs_info: # Agora usa o nome correto da variável
    print(f"Processando {rdc['nome']} ({rdc['mes_ano']})...")
    soup_rdc = get_soup(rdc['url'])
    time.sleep(2)

    if soup_rdc:
        text_rdc = extract_text_from_anvisa_page(soup_rdc)
        if not text_rdc:
            print(f"  Não foi possível extrair texto da {rdc['nome']}.")
            continue

        alterations = re.finditer(r'(LISTA\s*["\']([A-Z0-9]+)["\'])(.*?)(?=LISTA\s*["\']|Art\.|Parágrafo|\Z)', text_rdc, re.IGNORECASE | re.DOTALL)
        found_alteration_in_rdc = False
        for alt_match in alterations:
            current_list_name_rdc = alt_match.group(2).upper()
            list_content_text = alt_match.group(3)

            incluir_match = re.search(r'INCLUSÃO:\s*(.*?)(?=EXCLUSÃO:|ADENDO\s*\(ESPECÍFICO\s*PARA\s*A\s*LISTA\)|NOTAS\s*TÉCNICAS|\Z)', list_content_text, re.IGNORECASE | re.DOTALL)
            if incluir_match:
                found_alteration_in_rdc = True
                substances_to_include_text = incluir_match.group(1)
                # Corrigido para 'INCLUSÃO'
                novas_substancias = parse_substances_from_text(substances_to_include_text, current_list_name_rdc, 'INCLUSÃO', rdc['mes_ano'])
                if novas_substancias:
                    print(f"  {rdc['nome']}: Incluindo {len(novas_substancias)} em {current_list_name_rdc}.")
                    medicamentos_controlados.extend(novas_substancias)

            excluir_match = re.search(r'EXCLUSÃO:\s*(.*?)(?=INCLUSÃO:|ADENDO\s*\(ESPECÍFICO\s*PARA\s*A\s*LISTA\)|NOTAS\s*TÉCNICAS|\Z)', list_content_text, re.IGNORECASE | re.DOTALL)
            if excluir_match:
                found_alteration_in_rdc = True
                substances_to_exclude_text = excluir_match.group(1)
                # Corrigido para 'EXCLUSÃO'
                substancias_excluidas_info = parse_substances_from_text(substances_to_exclude_text, current_list_name_rdc, 'EXCLUSÃO', rdc['mes_ano'])

                if substancias_excluidas_info:
                    print(f"  {rdc['nome']}: Excluindo {len(substancias_excluidas_info)} de {current_list_name_rdc}.")
                    for item_excluir in substancias_excluidas_info:
                        found_to_exclude = False
                        for med_existente in medicamentos_controlados:
                            if med_existente['principio_ativo'] == item_excluir['principio_ativo'] and \
                               med_existente['lista'] == item_excluir['lista'] and \
                               med_existente['mes_ano_exclusao'] is None:
                                if med_existente['mes_ano_inclusao'] and rdc['mes_ano']:
                                    try:
                                        inc_m, inc_a = map(int, med_existente['mes_ano_inclusao'].split('/'))
                                        exc_m, exc_a = map(int, rdc['mes_ano'].split('/'))
                                        if (exc_a, exc_m) >= (inc_a, inc_m):
                                            med_existente['mes_ano_exclusao'] = rdc['mes_ano']
                                            found_to_exclude = True
                                            break
                                    except ValueError:
                                        print(f"    Aviso: Formato de data inválido ao comparar inclusão/exclusão para {med_existente['principio_ativo']}")
                                        med_existente['mes_ano_exclusao'] = rdc['mes_ano']
                                        found_to_exclude = True
                                        break
                        if not found_to_exclude:
                             print(f"    Aviso: Substância {item_excluir['principio_ativo']} (Lista {item_excluir['lista']}) marcada para exclusão pela {rdc['nome']}, mas não encontrada ativa ou já excluída.")
                             medicamentos_controlados.append({
                                 'principio_ativo': item_excluir['principio_ativo'],
                                 'lista': item_excluir['lista'],
                                 'mes_ano_inclusao': None,
                                 'mes_ano_exclusao': rdc['mes_ano']
                             })
        if not found_alteration_in_rdc:
             print(f"  Nenhuma alteração clara de INCLUSÃO/EXCLUSÃO encontrada no formato esperado na {rdc['nome']}.")

# 3. Processar Antimicrobianos (Base: RDC 20/2011)
print("Processando RDC 20/2011 para Antimicrobianos...")
url_rdc20_2011 = "https://bvsms.saude.gov.br/bvs/saudelegis/anvisa/2011/rdc0020_11_05_2011.html"
soup_rdc20 = get_soup(url_rdc20_2011)
time.sleep(2)

if soup_rdc20:
    text_rdc20 = soup_rdc20.body.get_text(separator='\n', strip=True) if soup_rdc20.body else ""
    mes_ano_rdc20 = "05/2011"

    anexo_i_rdc20_match = re.search(r'ANEXO I\s*\n\s*LISTA DE ANTIMICROBIANOS.*?SUJEITOS AO CONTROLE DA LEI Nº 5\.991/1973(.*?)(?=ANEXO II|\Z)', text_rdc20, re.IGNORECASE | re.DOTALL)
    anexo_ii_rdc20_match = re.search(r'ANEXO II\s*\n\s*LISTA DE ANTIMICROBIANOS DE USO RESTRITO A ESTABELECIMENTOS DE SAÚDE(.*?)(?=\Z)', text_rdc20, re.IGNORECASE | re.DOTALL)

    antimicrobianos_text_parts = []
    if anexo_i_rdc20_match:
        print("  Encontrado Anexo I na RDC 20/2011.")
        antimicrobianos_text_parts.append(anexo_i_rdc20_match.group(1))
    else:
        print("  Anexo I da RDC 20/2011 não encontrado no formato esperado.")

    if anexo_ii_rdc20_match:
        print("  Encontrado Anexo II na RDC 20/2011.")
        antimicrobianos_text_parts.append(anexo_ii_rdc20_match.group(1))
    else:
        print("  Anexo II da RDC 20/2011 não encontrado no formato esperado.")

    for part_text in antimicrobianos_text_parts:
        lines = part_text.split('\n')
        for line in lines:
            principio_ativo = line.strip()
            principio_ativo = re.sub(r'^\-\s*|^\*\s*', '', principio_ativo).strip()
            if not principio_ativo or len(principio_ativo) < 3 or principio_ativo.lower().startswith(("lista", "anexo", "observação", "(obs", "item", "substância", "classe terapêutica")): # Adicionado "classe terapêutica"
                continue
            ja_existe = any(m['principio_ativo'] == principio_ativo.upper() and m['lista'] == 'ANTIMICROBIANOS' for m in medicamentos_controlados)
            if not ja_existe:
                 medicamentos_controlados.append({
                    'principio_ativo': principio_ativo.upper(),
                    'lista': 'ANTIMICROBIANOS',
                    'mes_ano_inclusao': mes_ano_rdc20, # Data de inclusão para antimicrobianos
                    'mes_ano_exclusao': None
                })
    print(f"  Adicionados antimicrobianos da RDC 20/2011. Total agora: {len(medicamentos_controlados)}")
else:
    print("  Não foi possível buscar RDC 20/2011 para antimicrobianos.")

print("\nAVISO: O mapeamento para Portaria 344/98 não inclui atualizações entre 06/1998 e 12/2019, exceto as RDCs de 2020 fornecidas.")
print("AVISO: Para antimicrobianos, a lista é baseada na RDC 20/2011. Atualizações subsequentes até 2020 não foram processadas automaticamente e exigiriam análise de RDCs adicionais.")

# 4. Filtrar dados e preparar para CSV
dados_finais_csv = []
for med in medicamentos_controlados:
    incluir_no_csv = False
    ano_inclusao_val = 9999
    mes_inclusao_val = 12

    if med.get('mes_ano_inclusao'):
        try:
            mes_inc, ano_inc = map(int, med['mes_ano_inclusao'].split('/'))
            mes_inclusao_val = mes_inc
            ano_inclusao_val = ano_inc
            if ano_inc <= 2020:
                incluir_no_csv = True
            else:
                continue
        except (ValueError, AttributeError):
            if med['lista'] == 'ANTIMICROBIANOS' and med.get('mes_ano_inclusao') is None:
                print(f"Aviso: Antimicrobiano {med['principio_ativo']} sem data de inclusão clara, assumindo inclusão válida até 2020.")
                incluir_no_csv = True
            elif med.get('mes_ano_exclusao'):
                 incluir_no_csv = True
            else:
                print(f"Aviso: {med['principio_ativo']} (Lista {med['lista']}) com data de inclusão inválida ou ausente: {med.get('mes_ano_inclusao')}. Descartando.")
                continue
    elif med.get('mes_ano_exclusao'): # Se não tem inclusão mas tem exclusão (veio de RDC de exclusão de item não pego na P344)
        incluir_no_csv = True


    if incluir_no_csv and med.get('mes_ano_exclusao'):
        try:
            mes_exc, ano_exc = map(int, med['mes_ano_exclusao'].split('/'))
            if ano_exc > 2020:
                med['mes_ano_exclusao'] = None
            # Não precisamos mais da verificação de exclusão antes da inclusão aqui se o registro for mantido
            # para indicar a exclusão de algo não capturado na base.
        except (ValueError, AttributeError):
            print(f"Aviso: {med['principio_ativo']} (Lista {med['lista']}) com data de exclusão inválida: {med.get('mes_ano_exclusao')}. Mantendo como não excluída.")
            med['mes_ano_exclusao'] = None
    
    if incluir_no_csv:
        # Se mes_ano_inclusao for None mas existe mes_ano_exclusao, significa que é um item
        # que foi identificado como excluído por uma RDC, mas sua inclusão original
        # (provavelmente pré-2020 e não nas RDCs de 2020) não foi capturada.
        # Ainda assim, é válido para o CSV mostrar a exclusão.
        dados_finais_csv.append(med)


# 5. Salvar em CSV
df = pd.DataFrame(dados_finais_csv)
if not df.empty:
    df = df[['principio_ativo', 'lista', 'mes_ano_inclusao', 'mes_ano_exclusao']]
    df.sort_values(by=['principio_ativo', 'lista', 'mes_ano_inclusao', 'mes_ano_exclusao'], na_position='first', inplace=True) # na_position='first' para Nones em datas virem antes
    df.drop_duplicates(subset=['principio_ativo', 'lista', 'mes_ano_inclusao', 'mes_ano_exclusao'], keep='first', inplace=True)

nome_arquivo = 'mapeamento_lista_anvisa.csv'
try:
    df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig')
    print(f"\nDados salvos com sucesso em '{nome_arquivo}'")
    print(f"Total de registros: {len(df)}")
    if not df.empty:
        print("\nAmostra dos dados:")
        print(df.head())
except Exception as e:
    print(f"Erro ao salvar o arquivo CSV: {e}")
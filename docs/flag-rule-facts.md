# Insumos da regra da bandeira tarifária (`flag_rule_month`)

Etapa 2 da previsão da bandeira. Semente versionada em
`src/energy_pipeline/seeds/flag_rule_month.csv`, carregada por
`python -m energy_pipeline.market_cli flag-rules` (módulo `energy_pipeline.flag_rules`).

## O que a semente guarda

**Só fatos numéricos** dos boletins InfoBandeira Tarifária da CCEE: PLD mínimo e máximo, GH, GF,
GSF, PLD gatilho, GFOM, limites superiores das faixas, cor acionada, número, data e URL do
boletim, data do anúncio da ANEEL e data de revisão. **Os PDFs não são guardados nem hospedados.**
A URL é só um ponteiro para a página da CCEE. Texto dos boletins não é copiado.

> **Licença.** Os termos de uso da CCEE (13/10/2025) só liberam a reprodução de conteúdo público
> para fins sem fins lucrativos, citando a fonte. Não há licença aberta declarada para os
> boletins. Fatos numéricos com atribuição são o uso mais defensável, mas **é preciso parecer
> jurídico antes de monetizar** qualquer produto que dependa desses dados (Plano Pro, API paga).
> Atribuição na tela: "Fonte: CCEE, InfoBandeira Tarifária nº X".

## Cobertura

37 boletins lidos, nº 52 (ago/2022) a nº 102 (out/2026), em 51 possíveis nesse intervalo.
Sem boletim na semente, por **pendência do usuário** (baixar à mão ou passar a URL):

| Boletins | Meses |
|---|---|
| 57 | jan/2023 |
| 78 a 83 | out/2024 a mar/2025 |
| 85 a 91 | mai/2025 a nov/2025 (88 e 91 não foram achados na página da CCEE) |

Antes de ago/2022 (nº 1 a 51) não foram coletados. As cores de todos os meses estão em
`tariff_flag_monthly` (ANEEL, 2015 em diante); o que falta nesses meses são os insumos da regra.

## Colunas

As 17 primeiras são as de `flag_rule_month` (migração 0016 da API). Duas colunas extras ficam só
na semente e não vão ao banco: `announced_on` (dia do anúncio da ANEEL, útil para calcular os
prazos) e `pld_max_table15_brl_per_mwh`.

- `pld_max_brl_per_mwh` é o **teto efetivo**: o limite superior da última faixa na Tabela 16 do
  boletim. Em 2026 a Tabela 15 do boletim traz 758,27 em alguns meses (97 a 100 e 102), mas a
  Tabela 16 termina em 785,27 em todos, e as faixas só batem com 785,27. A coluna
  `pld_max_table15_brl_per_mwh` guarda o que a Tabela 15 imprime, para auditoria. Parece erro de
  digitação (758,27 contra 785,27), não confirmado.
- Quando a faixa seguinte não existe (GSF alto), o boletim imprime "-". Na semente, os limites
  das faixas ausentes repetem o teto (a última faixa existente termina no teto).
- `rule_version`: `B` até mar/2024, `C` a partir de abr/2024 (REH 3.306/2024). As constantes de
  cada versão foram **ajustadas nas faixas dos boletins, a confirmar no Anexo I da REH 3.306/2024**.
  Os boletins nº 61 a 67 só são reproduzidos pela versão B e nº 73 em diante só pela C; os de
  nº 68 a 72 reproduzem nas duas porque o GSF alto deixa tudo verde.
- `flag` vem de `tariff_flag_monthly` (ANEEL) e, em set/2024, é a cor revisada (`revised_on` =
  2024-09-04, Despacho ANEEL 2.668/2024). A Tabela 17 do boletim, onde a cor aparece, é imagem.

## Validação

No carregamento (`flag_rule_checks` e `cross_check`):

- faixas em ordem e dentro de PLD mín e máx; GSF = GH/GF a 0,005; cor coerente com a faixa do
  gatilho (meses sem GFOM e sem revisão);
- **cada mês da semente tem a mesma cor de `tariff_flag_monthly`**; qualquer divergência para a
  carga (a validação contra o banco roda quando o Postgres estiver disponível);
- o motor da regra (`spillway-forecast`, `flag_rule.py`) reproduz as faixas dos 37 boletins ao
  centavo e as 37 cores.

## Como atualizar a cada mês

Depois do anúncio da ANEEL (28 a 31 do mês anterior) e da publicação do boletim, o dono lê o
boletim à mão, acrescenta uma linha ao CSV e abre um PR. Nenhum robô baixa os PDFs (a CCEE
bloqueia automação e os termos limitam o uso).

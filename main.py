import pandas as pd
import numpy as np
from supabase import create_client

# 🔐 CONEXÃO
url = "https://woesgdgbnhjckgmgjxbt.supabase.co"
key = "YOUR_SUPABASE_KEY"

supabase = create_client(url, key)

# 📁 CAMINHO
base_path = r"C:\Users\Jonas Gabriel\Desktop\ENGENHARIA INTELIGENTE DE DADOS\TABELAS"

# =========================
# 🔧 LIMPEZA
# =========================
def limpar_df(df):
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)
    return df

# =========================
# 🔧 UPLOAD
# =========================
def upload_df(df, tabela):
    df = limpar_df(df)
    dados = df.to_dict(orient="records")

    print(f"\n📦 Enviando {tabela} | Linhas: {len(dados)}")

    for i in range(0, len(dados), 500):
        lote = dados[i:i+500]
        try:
            supabase.table(tabela).insert(lote).execute()
        except Exception as e:
            print(f"\n❌ ERRO em {tabela}: {e}")
            return

    print(f"✔ {tabela} enviada!")

# =========================
# 🔥 DIM CLIENTE
# =========================
df_cliente = pd.read_csv(f"{base_path}\\dim_cliente.csv")
df_cliente = df_cliente[["customer_id", "customer_state"]].drop_duplicates()
df_cliente = df_cliente.dropna(subset=["customer_id"])

# =========================
# 🔥 DIM PRODUTO
# =========================
df_produto = pd.read_csv(f"{base_path}\\dim_produto.csv")
df_produto = df_produto[["product_id", "nome_produto"]]
df_produto = df_produto.dropna(subset=["product_id"]).drop_duplicates()

# =========================
# 🔥 DIM VENDEDOR
# =========================
df_vendedor = pd.read_csv(f"{base_path}\\dim_vendedor.csv")
df_vendedor = df_vendedor[["seller_id"]].drop_duplicates()
df_vendedor = df_vendedor.dropna(subset=["seller_id"])

# =========================
# 🔥 DIM TEMPO
# =========================
df_tempo = pd.read_csv(f"{base_path}\\dim_tempo.csv")

df_tempo["data"] = pd.to_datetime(df_tempo["order_purchase_timestamp"], errors="coerce")
df_tempo = df_tempo.dropna(subset=["data"])

df_tempo["ano"] = df_tempo["data"].dt.year
df_tempo["mes"] = df_tempo["data"].dt.month
df_tempo["dia"] = df_tempo["data"].dt.day

df_tempo["id_tempo"] = df_tempo["data"].astype(str)
df_tempo["data"] = df_tempo["data"].astype(str)

df_tempo = df_tempo[["id_tempo", "data", "ano", "mes", "dia"]].drop_duplicates()

# =========================
# 🔥 FATO VENDAS
# =========================
df_fato = pd.read_csv(f"{base_path}\\fato_vendas.csv")

df_fato["data_compra"] = pd.to_datetime(df_fato["order_purchase_timestamp"], errors="coerce")
df_fato["data_entrega"] = pd.to_datetime(df_fato["order_delivered_customer_date"], errors="coerce")

df_fato["id_tempo"] = df_fato["data_compra"].astype(str)

df_fato["tempo_entrega"] = (
    df_fato["data_entrega"] - df_fato["data_compra"]
).dt.days

# garantir colunas
df_fato["price"] = df_fato.get("price", 0)
df_fato["freight_value"] = df_fato.get("freight_value", 0)
df_fato["payment_value"] = df_fato.get("payment_value", 0)
df_fato["payment_installments"] = df_fato.get("payment_installments", 1)
df_fato["review_score"] = df_fato.get("review_score", 3)
df_fato["order_id"] = df_fato.get("order_id", None)

# 🔥 REMOVER IDS QUE NÃO EXISTEM NAS DIMENSÕES (ESSENCIAL PRA FK)
df_fato = df_fato[
    df_fato["customer_id"].isin(df_cliente["customer_id"]) &
    df_fato["product_id"].isin(df_produto["product_id"]) &
    df_fato["seller_id"].isin(df_vendedor["seller_id"]) &
    df_fato["id_tempo"].isin(df_tempo["id_tempo"])
]

# selecionar colunas
df_fato = df_fato[[
    "order_id",
    "customer_id",
    "product_id",
    "seller_id",
    "id_tempo",
    "price",
    "freight_value",
    "payment_value",
    "payment_installments",
    "review_score",
    "tempo_entrega"
]]

# 🔥 LIMPEZA FINAL (ANTI JSON)
df_fato = df_fato.replace([np.inf, -np.inf], None)
df_fato = df_fato.where(pd.notnull(df_fato), None)

for col in ["price", "freight_value", "payment_value", "tempo_entrega"]:
    df_fato[col] = pd.to_numeric(df_fato[col], errors="coerce").fillna(0)


df_fato["review_score"] = pd.to_numeric(
    df_fato["review_score"], errors="coerce"
).fillna(3).astype(int)

df_fato["payment_installments"] = pd.to_numeric(
    df_fato["payment_installments"], errors="coerce"
).fillna(1).astype(int)

df_fato["tempo_entrega"] = pd.to_numeric(
    df_fato["tempo_entrega"], errors="coerce"
).fillna(0).astype(int)

df_fato = df_fato.astype(object)

# =========================
# 🚀 CARGA
# =========================
print("\n🚀 Enviando para Supabase...\n")

upload_df(df_cliente, "dim_cliente")
upload_df(df_produto, "dim_produto")
upload_df(df_vendedor, "dim_vendedor")
upload_df(df_tempo, "dim_tempo")
upload_df(df_fato, "fato_vendas")

print("\n🎉 DW 100% ENVIADO")
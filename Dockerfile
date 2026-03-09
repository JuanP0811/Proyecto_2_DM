FROM mageai/mageai:latest

# Actualiza Mage dentro de la imagen (para evitar el bug del DBT block)
RUN pip install --no-cache-dir -U mage-ai

# Asegura adapter de Postgres
RUN pip install --no-cache-dir -U dbt-postgres
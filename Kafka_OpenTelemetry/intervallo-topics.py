import pandas as pd
import matplotlib.pyplot as plt



df1 = pd.read_csv('/home/alessio/Documents/latenzaMedia.csv')
plt.figure(figsize=(6, 6))
plt.bar(df1['Topic'], df1['Latenza Media'], color='orange', width=0.6)
plt.xlabel('Percorso')
plt.ylabel('Latenza (μs)')
plt.title('Latenza dei messaggi media per ogni percorso')
plt.tight_layout()


df = pd.read_csv('/home/alessio/Documents/latenza_ordinata.csv')
# Separare i dati in due parti
#df_T1_T2 = df.iloc[:200].copy()
#df_T1_T5 = df.iloc[200:].copy()

df_T1_T2 = df.iloc[:1400].copy()
df_T1_T5 = df.iloc[1400:].copy()

# Riassegna gli indici in modo che entrambi i percorsi siano nei primi 200 punti
df_T1_T2.index = range(1400)
df_T1_T5.index = range(1400)

# **Convertire la latenza da microsecondi a millisecondi**
df_T1_T2['Latenza Media'] = df_T1_T2['Latenza Media'] / 1000
df_T1_T5['Latenza Media'] = df_T1_T5['Latenza Media'] / 1000

# Calcola la media mobile per ciascuna metà
window_size = 100  # Dimensione della finestra per la media mobile
df_T1_T2['Media Mobile'] = df_T1_T2['Latenza Media'].rolling(window=window_size).mean()
df_T1_T5['Media Mobile'] = df_T1_T5['Latenza Media'].rolling(window=window_size).mean()

# Aggiungi una colonna per identificare i percorsi
df_T1_T2['Percorso'] = 'T1-T2'
df_T1_T5['Percorso'] = 'T1-T5'

# Concatenare i dati
df_combined = pd.concat([df_T1_T2, df_T1_T5])

# Crea un grafico
plt.figure(figsize=(14, 7))


# Colori per i punti e la media mobile
colori_punti = {
    'T1-T2': '#1fb4ad',  # Colore blu per T1-T2
    'T1-T5': '#e3b344',  # Colore arancione per T1-T5
}

# Colori più scuri per la media mobile
colori_media_mobile = {
    'T1-T2': '#0d47a1',  # Blu più scuro per la media mobile di T1-T2
    'T1-T5': '#e65100',  # Arancione più scuro per la media mobile di T1-T5
}

# Grafico per Latenza Media con punti (riduzione della dimensione dei punti)
for percorso in df_combined['Percorso'].unique():
    subset = df_combined[df_combined['Percorso'] == percorso]
    plt.scatter(subset.index, subset['Latenza Media'], label=f'{percorso}', alpha=0.2, s=20, color=colori_punti[percorso])
    plt.plot(subset.index, subset['Latenza Media'], label=f'{percorso}', alpha=0.2)

# Grafico per Media Mobile con linee più visibili
for percorso in df_combined['Percorso'].unique():
    subset = df_combined[df_combined['Percorso'] == percorso]
    plt.plot(subset.index, subset['Media Mobile'], label=f'{percorso} Media Mobile', linestyle='-', linewidth=2.5, alpha=0.9, color=colori_media_mobile[percorso])


# Aggiungi titoli e legende
plt.title('Grafico di Media Mobile per Percorsi T1-T2 e T1-T5')
plt.xlabel('Messaggi')
plt.ylabel('Latenza (ms)')
plt.legend()


plt.show()



plt.show()

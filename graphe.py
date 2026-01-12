import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import colors
from sklearn.linear_model import LinearRegression

# Charger le fichier sans en-tête
file_path = "result_deck"  # à adapter
columns = [
    "archetype",
    "count",
    "win",
    "count_source",
    "count_target",
    "prevision"
]

df = pd.read_csv(file_path, sep=";", header=None, names=columns)

# Variables explicative et cible
X = df[["count"]]
y = df["prevision"]

# Régression linéaire
model = LinearRegression()
model.fit(X, y)
y_pred = model.predict(X)

print("Coefficient :", model.coef_[0])
print("Intercept :", model.intercept_)
print("R² :", model.score(X, y))

# Ratio réel / prédit
epsilon = 1e-6
ratio = y / (y_pred + epsilon)

# Scatter plot avec échelle logarithmique
plt.figure(figsize=(8, 6))

# Norm logarithmique pour la couleur
vmin = max(ratio.min(), 1e-2)  # minimum pour éviter log(0)
vmax = ratio.max()
norm = colors.LogNorm(vmin=vmin, vmax=vmax)

scatter = plt.scatter(
    df["count"],
    y,
    c=ratio,
    cmap="viridis",
    norm=norm,
    s=2,
)

# Droite de régression
df_sorted = df.sort_values("count")
y_pred_sorted = model.predict(df_sorted[["count"]])
plt.plot(
    df_sorted["count"],
    y_pred_sorted,
    color="darkcyan",
    linewidth=2,
    label="Régression linéaire"
)
# Coefficient et intercept
a = model.coef_[0]
b = model.intercept_

# Formater l'équation
equation_text = f"y = {a:.2f} x + {b:.2f}"


# Colorbar logarithmique
cbar = plt.colorbar(scatter)
cbar.set_label("Ratio")
cbar.set_ticks([1e-2, 1e-1, 1, 10, 100])  # ticks log espacés
cbar.set_ticklabels([r"$10^{-2}$", r"$10^{-1}$", r"$10^{0}$", r"$10^{1}$", r"$10^{2}$"])

# Labels et grille
plt.xlabel("Mesure")
plt.ylabel("Estimation")
plt.title("scatter + regression linéaire")
plt.text(
    0.05, 0.95,         # position en fraction de la figure (0 à 1)
    equation_text,
    transform=plt.gca().transAxes,  # coordonnées relatives à l'axe
    fontsize=12,
    color="black",
    verticalalignment='top'
)
plt.grid(False)
plt.show()

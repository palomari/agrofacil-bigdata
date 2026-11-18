import csv
import random
from pathlib import Path

NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gisele", "Hugo", "Isabela", "Jorge"]
SOBRENOMES = ["Almeida", "Barros", "Cardoso", "Duarte", "Ferreira", "Gomes", "Machado", "Nogueira", "Rocha", "Teixeira"]
MUNICIPIOS = ["3509502", "3543402", "3538709"]
CULTURAS = ["Cana-de-açúcar", "Milho", "Soja", "Laranja"]


def dv(digitos, pesos_ini):
    soma = sum(int(d) * p for d, p in zip(digitos, range(pesos_ini, 1, -1)))
    resto = (soma * 10) % 11
    return 0 if resto == 10 else resto


def cpf_invalido(rng):
    base = [rng.randint(0, 9) for _ in range(9)]
    d1_correto = dv(base, 10)
    d1 = (d1_correto + 1) % 10
    d2 = dv(base + [d1], 11)
    return "".join(map(str, base + [d1, d2]))


def main(qtd=8, semente=2026):
    rng = random.Random(semente)
    linhas, vistos = [], set()
    while len(linhas) < qtd:
        cpf = cpf_invalido(rng)
        if cpf in vistos:
            continue
        vistos.add(cpf)
        nome = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
        linhas.append((cpf, nome, rng.choice(MUNICIPIOS), rng.choice(CULTURAS)))

    saida = Path(__file__).resolve().parent.parent / "amostras" / "produtores_simulados.csv"
    saida.parent.mkdir(exist_ok=True)
    with saida.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cpf_produtor", "nome_produtor", "codigo_ibge", "cultura_principal"])
        w.writerows(linhas)
    print(f"{len(linhas)} produtores simulados gravados em {saida}\n")
    print("produtores = [")
    for l in linhas:
        print(f"    {l!r},")
    print("]")


if __name__ == "__main__":
    main()
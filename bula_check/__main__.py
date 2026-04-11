from pathlib import Path

from bula_check.bulas import Bula
from bula_check.llm import precheck_llm
from bula_check.llm import validate_llm


def _ask_claim():
    return input("Faça alguma alegação sobre um medicamento: ")


def main():
    print("Bem vindx ao BulaCheck, o verificador de veracidade sobre medicamentos.")
    print("")

    while True:
        claim = _ask_claim()

        print("Processando...")
        print("")

        precheck = precheck_llm(claim)

        print(precheck.summary)

        # temporary, just for debug
        if precheck.needs_evidence and "paracetamol" in claim.lower():
            print("Buscando mais informações...")
            print("")

            bula = Bula.read_from_json(
                Path("inputs/bulas/json/paracetamol__prati_donaduzzi__cia_ltda.json")
            )

            validate = validate_llm(claim, bula.drug_name, bula.raw_text)

            print(validate.justification)
            print("")
            # print("Achei esses trechos na bula...")
            # print("")

            # for evidence in validate.evidence_used:
            #     print(evidence)

        elif precheck.needs_evidence and not "paracetamol" in claim.lower():
            print("Preciso de mais bulas para avaliar...")

        else:
            print(precheck.justification)

        continuar = input("\nDeseja continuar? (s/n): ").strip().lower()
        if continuar != "s":
            print("Encerrando...")
            break


if __name__ == "__main__":
    main()

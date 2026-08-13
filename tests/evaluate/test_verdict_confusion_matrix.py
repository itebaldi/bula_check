from bula_check.evaluate import _verdict_confusion_matrix


def test_verdict_confusion_matrix__counts_hits_on_the_diagonal():
    matrix = _verdict_confusion_matrix(
        [("confirmed", "confirmed"), ("refuted", "refuted")]
    )
    assert matrix["confirmed"]["confirmed"] == 1
    assert matrix["refuted"]["refuted"] == 1
    assert matrix["inconclusive"]["inconclusive"] == 0


def test_verdict_confusion_matrix__missing_verdict_gets_its_own_column():
    matrix = _verdict_confusion_matrix(
        [("inconclusive", ""), ("confirmed", ""), ("refuted", "confirmed")]
    )
    assert matrix["inconclusive"]["sem_resposta"] == 1
    assert matrix["confirmed"]["sem_resposta"] == 1
    assert matrix["inconclusive"]["inconclusive"] == 0
    assert matrix["refuted"]["confirmed"] == 1


def test_verdict_confusion_matrix__diagonal_matches_strict_accuracy():
    pairs = [
        ("confirmed", "confirmed"),
        ("refuted", "inconclusive"),
        ("inconclusive", ""),
        ("inconclusive", "inconclusive"),
    ]
    matrix = _verdict_confusion_matrix(pairs)
    diagonal = sum(matrix[label][label] for label in matrix)
    assert diagonal == sum(expected == predicted for expected, predicted in pairs)


def test_verdict_confusion_matrix__has_three_rows_and_four_columns():
    matrix = _verdict_confusion_matrix([])
    assert list(matrix) == ["confirmed", "refuted", "inconclusive"]
    assert list(matrix["confirmed"]) == [
        "confirmed",
        "refuted",
        "inconclusive",
        "sem_resposta",
    ]


def test_verdict_confusion_matrix__unknown_expected_label_falls_back_to_inconclusive():
    matrix = _verdict_confusion_matrix([("", "confirmed")])
    assert matrix["inconclusive"]["confirmed"] == 1

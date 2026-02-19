"""Tests du module normalizer — normalisation des noms et scoring."""

import pytest

from s3dedup.normalizer import name_quality_score, normalize_name


class TestNormalizeName:
    """Tests de normalize_name()."""

    def test_simple_name(self):
        assert normalize_name("photo.jpg") == "photo.jpg"

    def test_lowercase(self):
        assert normalize_name("Photo.JPG") == "photo.jpg"

    def test_strip_accents(self):
        assert normalize_name("photo été.jpg") == "photo ete.jpg"

    def test_strip_parenthesis_suffix(self):
        assert normalize_name("photo (1).jpg") == "photo.jpg"
        assert normalize_name("photo (2).jpg") == "photo.jpg"

    def test_strip_underscore_number_suffix(self):
        assert normalize_name("photo_1.jpg") == "photo.jpg"
        assert normalize_name("photo_2.jpg") == "photo.jpg"

    def test_strip_copy_suffix_fr(self):
        assert normalize_name("photo - Copie.jpg") == "photo.jpg"
        assert normalize_name("photo - copie.jpg") == "photo.jpg"

    def test_strip_copy_suffix_en(self):
        assert normalize_name("photo - Copy.jpg") == "photo.jpg"
        assert normalize_name("photo_copy.jpg") == "photo.jpg"

    def test_strip_leading_trailing_spaces(self):
        assert normalize_name(" photo .jpg") == "photo.jpg"

    def test_normalize_multiple_spaces(self):
        assert normalize_name("photo  été  2024.jpg") == "photo ete 2024.jpg"

    def test_combined_normalization(self):
        """Accents + suffixe de copie + espaces."""
        assert normalize_name("Photo été (1).jpg") == "photo ete.jpg"

    def test_preserves_path_takes_basename(self):
        """normalize_name extrait le basename."""
        assert normalize_name("dossier/sous/photo.jpg") == "photo.jpg"

    def test_no_extension(self):
        assert normalize_name("README") == "readme"

    def test_complex_accents(self):
        """Caractères accentués variés."""
        assert normalize_name("café crème.txt") == "cafe creme.txt"


class TestNameQualityScore:
    """Tests de name_quality_score()."""

    def test_clean_name_zero(self):
        assert name_quality_score("photo.jpg") == 0

    def test_mojibake_penalty(self):
        """Encodage cassé latin-1→UTF-8."""
        score = name_quality_score("photo Ã©tÃ©.jpg")
        assert score >= 10

    def test_copy_suffix_penalty(self):
        score = name_quality_score("photo (1).jpg")
        assert score >= 5

    def test_leading_trailing_spaces_penalty(self):
        score = name_quality_score(" photo.jpg")
        assert score >= 2
        score2 = name_quality_score("photo .jpg")
        assert score2 >= 2

    def test_multiple_spaces_penalty(self):
        score = name_quality_score("photo  2024.jpg")
        assert score >= 1

    def test_cumulative_penalties(self):
        """Mojibake + suffixe de copie = pénalités cumulées."""
        score = name_quality_score("photo Ã©tÃ© (1).jpg")
        assert score >= 15

    def test_clean_beats_dirty(self):
        """Un nom propre a un meilleur score qu'un nom dégradé."""
        assert name_quality_score("photo.jpg") < \
            name_quality_score("photo (1).jpg")
        assert name_quality_score("photo.jpg") < \
            name_quality_score(" photo.jpg")
        assert name_quality_score("photo.jpg") < \
            name_quality_score("photo Ã©tÃ©.jpg")

    def test_path_uses_basename(self):
        """Le score est calculé sur le basename."""
        assert name_quality_score("dossier/photo.jpg") == 0

    @pytest.mark.parametrize("name,expected_min", [
        ("song_copy.mp3", 5),
        ("song - Copie.mp3", 5),
        ("song_1.mp3", 5),
    ])
    def test_various_copy_suffixes(self, name, expected_min):
        assert name_quality_score(name) >= expected_min

"""Tests for the figshare scraper module."""

import pytest

import mdverse_scrapers.scrapers.figshare as figshare_scraper


@pytest.mark.network
def test_extract_files_from_zip_file():
    """Test the extract_files_from_zip_file function."""
    expected_file_names = [
        "topologies/martini/betacarotene-CG.itp",
        "topologies/martini/chlorophyll_a-CG.itp",
        "topologies/martini/heme-CG.itp",
        "topologies/martini/pheophytin-CG.itp",
        "topologies/martini/plastoquinone-CG.itp",
        "topologies/martini/table_BCR456.xvg",
        "topologies/martini/ubiquinone-CG.itp",
        "topologies/gromos/betacarotene-UA-Zhang.itp",
        "topologies/gromos/betacarotene-UA.itp",
        "topologies/gromos/chlorophyll_a-UA.itp",
        "topologies/gromos/heme-UA.itp",
        "topologies/gromos/pheophytin-UA.itp",
        "topologies/gromos/plastoquinone-UA.itp",
        "topologies/gromos/ubiquinone-UA.itp",
    ]
    file_names = figshare_scraper.extract_files_from_zip_file("3788686")
    assert file_names == expected_file_names


def test_extract_files_from_json_response():
    """Test the extract_files_from_json_response function.

    Example of JSON response:
    https://figshare.com/ndownloader/files/3788686/preview/3788686/structure.json
    """
    json_response = {
        "files": [],
        "dirs": [
            {
                "files": [],
                "path": "topologies",
                "dirs": [
                    {
                        "files": [
                            {"path": "topologies/martini/betacarotene-CG.itp"},
                            {"path": "topologies/martini/chlorophyll_a-CG.itp"},
                            {"path": "topologies/martini/heme-CG.itp"},
                            {"path": "topologies/martini/pheophytin-CG.itp"},
                            {"path": "topologies/martini/plastoquinone-CG.itp"},
                            {"path": "topologies/martini/table_BCR456.xvg"},
                            {"path": "topologies/martini/ubiquinone-CG.itp"},
                        ],
                        "path": "topologies/martini",
                        "dirs": [],
                    },
                    {
                        "files": [
                            {"path": "topologies/gromos/betacarotene-UA-Zhang.itp"},
                            {"path": "topologies/gromos/betacarotene-UA.itp"},
                            {"path": "topologies/gromos/chlorophyll_a-UA.itp"},
                            {"path": "topologies/gromos/heme-UA.itp"},
                            {"path": "topologies/gromos/pheophytin-UA.itp"},
                            {"path": "topologies/gromos/plastoquinone-UA.itp"},
                            {"path": "topologies/gromos/ubiquinone-UA.itp"},
                        ],
                        "path": "topologies/gromos",
                        "dirs": [],
                    },
                ],
            }
        ],
        "path": "ROOT",
    }
    file_names = figshare_scraper.extract_files_from_json_response(json_response)
    expected_file_names = [
        "topologies/martini/betacarotene-CG.itp",
        "topologies/martini/chlorophyll_a-CG.itp",
        "topologies/martini/heme-CG.itp",
        "topologies/martini/pheophytin-CG.itp",
        "topologies/martini/plastoquinone-CG.itp",
        "topologies/martini/table_BCR456.xvg",
        "topologies/martini/ubiquinone-CG.itp",
        "topologies/gromos/betacarotene-UA-Zhang.itp",
        "topologies/gromos/betacarotene-UA.itp",
        "topologies/gromos/chlorophyll_a-UA.itp",
        "topologies/gromos/heme-UA.itp",
        "topologies/gromos/pheophytin-UA.itp",
        "topologies/gromos/plastoquinone-UA.itp",
        "topologies/gromos/ubiquinone-UA.itp",
    ]
    assert file_names == expected_file_names

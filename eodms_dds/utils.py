from typing import Union, Dict, List


def get_service_uuid(data: Union[str, Dict, List[Dict]]) -> Union[Dict[str, str], List[Dict[str, str]]]:
    """
    Extract archiveId and collectionId from input.

    Args:
        data: Either
            - a string (archiveId only),
            - a dict containing "archiveId" and "collectionId",
            - a list of such dicts.

    Returns:
        Dict[str, str] if single entry,
        List[Dict[str, str]] if multiple entries.
    """

    def extract(d: Dict) -> Dict[str, str]:
        return {"archiveId": d.get("archiveId", ""), "collectionId": d.get("collectionId", "")}

    if isinstance(data, str):
        return {"archiveId": data, "collectionId": ""}

    if isinstance(data, dict):
        return extract(data)

    if isinstance(data, list):
        results = [extract(item) for item in data if isinstance(item, dict)]
        return results if len(results) > 1 else results[0] if results else {}

    raise ValueError("Unsupported input type.")

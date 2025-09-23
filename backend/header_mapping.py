import json
import pandas as pd
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

from semantic_kernel.contents.chat_history import ChatHistory
from semantic_kernel.contents.chat_message_content import ChatMessageContent
from semantic_kernel.contents.utils.author_role import AuthorRole
from semantic_kernel.connectors.ai.open_ai import AzureChatCompletion, AzureChatPromptExecutionSettings

load_dotenv()
CANONICAL_HEADERS = {
    "SOURCE_SYSTEM": None,
    "CUSTOMER_NUMBER": None,
    "CUSTOMER_NAME": None,
    "DUNS_NUMBER": None,
    "TAXPAYER_ID": None,
    "VAT_RGSTRN_ID": None,
    "ACCOUNT_TYPE": None,
    "ACCOUNT_ESTABLISHED_DATE": None,
    "CUSTOMER_PROFILE_CLASS": None,
    "CAPITALIQ_ID": None,
    "TOTALMAXDATEACTIVITY": None,
    "ADDRESS_SEQ_NUM": None,
    "SITE_NAME": None,
    "ACCOUNT_ADDRESS_SET": None,
    "LOCATION_SOURCE_REF": None,
    "ADDRESS1": None,
    "ADDRESS2": None,
    "ADDRESS3": None,
    "ADDRESS4": None,
    "CITY": None,
    "STATE": None,
    "PROVINCE": None,
    "POSTAL": None,
    "COUNTY": None,
    "COUNTRY": None,
    "PERSON_NUMBER": None,
    "SALUTARY_INTRODUCTION": None,
    "FIRST_NAME": None,
    "LAST_NAME": None,
    "JOB_TITLE": None,
    "BILL_TO_FLG": None,
    "SHIP_TO_FLG": None,
    "SOLD_TO_FLG": None,
    "PHONE_NUMBER": None,
    "PHONE": None,
    "MOBILE": None,
    "PHONE_EXTENSION": None,
    "E_MAIL_ADDRESS": None,
    "WEB_URL": None,
    "TRANSACTION_DATE": None,
}


def _extract_json_from_response(raw_response: str) -> str:
    start = raw_response.find("{")
    end = raw_response.rfind("}") + 1
    if start == -1 or end == -1:
        raise ValueError("No valid JSON found in response")
    return raw_response[start:end]


def _validate_and_fix_result(
    result: Dict[str, Optional[str]], headers: List[str]
) -> Dict[str, Optional[str]]:
    fixed_result = {}
    canonical_keys = set(CANONICAL_HEADERS.keys())

    for header in headers:
        value = result.get(header)
        if value in canonical_keys:
            fixed_result[header] = value
        else:
            fixed_result[header] = None
    return fixed_result

async def _ai_classify(
    headers: List[str], chat_completion: AzureChatCompletion
) -> Dict[str, Optional[str]]:
    history = ChatHistory()

    system_prompt = f"""
    You are a data normalization assistant that MUST return a valid JSON dictionary.

    **CRITICAL INSTRUCTIONS:**
    1. You MUST return ONLY a valid JSON dictionary, nothing else
    2. The JSON keys must be the exact original headers from the input
    3. The JSON values must be either a matching canonical header or null
    4. Do not include any explanation, commentary, or text outside the JSON
    5. Ensure the JSON is properly formatted and parseable

    **CANONICAL HEADERS:**
    {", ".join(CANONICAL_HEADERS.keys())}

    **MATCHING RULES:**
    - Direct matches: exact string matches (case-insensitive)
    - Fuzzy matches: similar strings with typos, abbreviations, or formatting differences
    - Semantic matches: different words with same meaning (e.g., "fname" → "first_name")
    - If no reasonable match exists, use null
    **EXAMPLE INPUT:** ["usr_name", "email_addr", "random_field"]
    **EXAMPLE OUTPUT:** {{"usr_name": "username", "email_addr": "email", "random_field": null}}
    Remember: Return ONLY the JSON dictionary, no other text.
    """

    history.add_message(ChatMessageContent(role=AuthorRole.SYSTEM, content=system_prompt))

    user_message = f"""Headers to classify: {json.dumps(headers)}

    Return the mapping as a JSON dictionary where each key is an original header and each value is either a matching canonical header or null."""

    history.add_message(ChatMessageContent(role=AuthorRole.USER, content=user_message))
    execution_settings = AzureChatPromptExecutionSettings(
        temperature=0.1,
        max_tokens=2000
    )

    try:
        reply = await chat_completion.get_chat_message_content(
            chat_history=history,
            settings=execution_settings
        )

        raw_response = reply.content.strip()
        json_response = _extract_json_from_response(raw_response)
        result = json.loads(json_response)

        validated_result = _validate_and_fix_result(result, headers)
        return validated_result

    except Exception as e:
        print(f"Error in AI classification: {e}")
        return {header: None for header in headers}

async def main():
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    deployment = os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT")

    if not endpoint or not api_key or not deployment:
        raise ValueError("Missing required environment variables in .env file")

    chat_completion = AzureChatCompletion(
        service_id="chat-gpt",
        deployment_name=deployment,
        endpoint=endpoint,
        api_key=api_key,
    )

    df = pd.read_excel("PS94_Customer.xlsx") 
    headers = df.columns.tolist()

    result = await _ai_classify(headers, chat_completion)

    print("Canonical Mapping:")
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

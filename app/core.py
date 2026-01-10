from google import genai
from google.genai import types
import os

from app.utils import get_file_context, get_context_log

client = genai.Client(api_key=os.getenv('API_KEY'))

def get_error_files(context: str) -> list[str]:
    file = client.models.generate_content(
        model="gemini-2.5-flash",
        config=types.GenerateContentConfig(
            system_instruction="""
            <Instruction>
                You need to identify the file in the log that is causing the error and output only its name.
                The file name may appear in the log after the line "Database error in the core_customer model (for example)".
                There may be multiple files, so separate them in a comma-separated list (e.g. file1.ext, file2.ext).
            </Instruction>
            <Response Format>
                filename.ext
            </Response Format>"""),
        contents=context
    )

    if file.text is None:
        raise ValueError('No error file identified.')

    return file.text if ',' not in file.text else list(map(str.strip, file.text.split(',')))

def send_for_llm(context: str, file: str) -> str:
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        config=types.GenerateContentConfig(
            system_instruction="""
            <Instruction>
                You are a code correction assistant in DBT project. Your task is to identify errors in the provided code and suggest solutions. 
                Frame all proposed changes within <solution> tags. Always provide only one solution. Do not explain the solution in excessive detail.
                If the error is related to database content, start with DB_ERROR, then provide SQL instructions for correction.
                EDIT ONLY THE CODE THAT COMES AFTER "SOURCE filename:".
                IF THERE ARE SEVERAL FILES, WRITE SEVERAL SOLUTIONS LIKE IN [Response Format]. SEPARATE THEM WITH '----' FORMAT.
            </Instruction>
            <Important>
            DO NOT USE CODE that places in shops_dwh/target/*.
            DO NOT TOUCH config() if error is not related to it.
            DO NOT TOUCH ref() if error is not related to it.
            </Important>
            <Response Format> 
                Error found in file filename.ext, here is the proposed solution:
                    <solution>
                    Complete correction code to replace the original
                    </solution>
                    <file>
                        filename.ext OR schema.table
                    </file>
            </Response Format>"""),
        contents=context + f'\n{get_file_context(file)}'
    )

    return response.text

def get_solution() -> str:
    context = get_context_log()

    if isinstance(context, list):
        context_str = '\n'.join(context)
    else:
        context_str = str(context)

    return send_for_llm(context=context_str, file=get_error_files(context_str))
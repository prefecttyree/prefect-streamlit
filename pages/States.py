import streamlit as st
from openai import OpenAI
from prefect import task, flow
from prefect.task_runners import ThreadPoolTaskRunner
# for type hints only

from prefect import Task
from prefect.context import TaskRun
from prefect.states import State

def check_task_state(tsk: Task, run: TaskRun, state: State) -> None:
    """
    A task hook that checks the state of the task.
    
    This task hook examines the state of the current task and logs its status and result.
    It can be used as a on_completion, on_failure, or on_running hook for any task.
    
    Returns:
        None
    """
    task_run = TaskRun.get_current()
    state = task_run.state
    
    if isinstance(state, State):
        if state.is_completed():
            result = state.result()
            print(f"Task {task_run.task.name} completed. Result: {result}")
            
            # Check if the task result is a valid OpenAI client (indicating valid API key)
            if isinstance(result, OpenAI):
                print("API key is valid.")
                return True
            else:
                print("Invalid API key.")
                return False
        elif state.is_failed():
            print(f"Task {task_run.task.name} failed. Exception: {state.exception}")
            return False
        else:
            print(f"Task {task_run.task.name} is running.")
            return False
    else:
        print(f"Unable to determine state for task {task_run.task.name}")
        return False
        
# Function to check state manually in main flow
def check_task_state_manually(task_future):
    """
    Check the state of a Prefect task manually.
    
    Args:
        task_future: The future object returned by the Prefect task execution.
    
    Returns:
        tuple: The state of the task and its result (or exception).
    """
    #state = task_future.wait()
    if task_future.is_completed():
        result = task_future.result()
        return "Completed", result
    elif task_future.is_failed():
        return "Failed", task_future.exception()
    else:
        return "Running", None
    
@task(name="Validate API Key", log_prints=True,on_failure=[check_task_state], on_completion=[check_task_state])
def validate_api_key(openai_api_key: str):
    """
    Validate the OpenAI API key.
    
    This task attempts to list OpenAI models using the provided API key.
    If successful, it confirms the API key is valid.
    
    Args:
        api_key (str): The OpenAI API key to validate.
    
    Returns:
        bool: True if the API key is valid, False otherwise.
    """
    if openai_api_key:
        try:
            client = OpenAI(api_key=openai_api_key)
            client.models.list()
            return client
        except Exception as e:
            return (f'Error validating API key: {e}')

@task(name="Get ChatGPT Response", log_prints=True,on_failure=[check_task_state], on_completion=[check_task_state])
def get_chatgpt_response(prompt, client):
    """
    Get a response from ChatGPT using the OpenAI API.
    
    This task sends a prompt to the ChatGPT model and returns its response.
    
    Args:
        prompt (str): The input prompt for ChatGPT.
    
    Returns:
        str: The response from ChatGPT or an error message if the API call fails.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"An error occurred: {str(e)}"


@flow(name="ChatGPT Interaction", task_runner=ThreadPoolTaskRunner())
def main():
    """
    Main flow for ChatGPT interaction.
    
    This flow orchestrates the entire process of validating the API key,
    getting user input, and interacting with ChatGPT. It uses Streamlit
    for the user interface and Prefect for workflow management.
    
    The flow performs the following steps:
    1. Collect the OpenAI API key from the user
    2. Validate the API key using the validate_api_key task
    3. If the key is valid, allow the user to input a message
    4. Send the user's message to ChatGPT using the get_chatgpt_response task
    5. Display the response from ChatGPT
    
    The flow uses the check_task_state task to monitor the progress and
    results of the validate_api_key and get_chatgpt_response tasks.
    """
    st.title('ChatGPT Interaction')

    api_key = st.text_input("Enter your OpenAI API Key:", type="password")
    
    if api_key:
        with st.spinner('Validating API Key...'):
            # Submit the validate_api_key task to Prefect for asynchronous execution
            # This returns a Future object that we can use to track the task's progress
            client = validate_api_key.submit(api_key)
            print(f'client: {client}')
            # Use the check_task_state function to get the current state and result of the validation task
            # This allows us to handle different states (Completed, Failed, Running) appropriately
            # Check the state of the task and verify if the API key is valid
            print(f'checking state manually: {client}')            
            is_valid = check_task_state_manually(client)
        if is_valid:
            st.success("Valid API Key!")
            
            user_input = st.text_area("Enter your message:")
            
            if st.button("Submit"):
                if user_input:
                    with st.spinner('Getting response from ChatGPT...'):
                        # Submit the get_chatgpt_response task to Prefect for execution
                        # This returns a Future object that we can use to track the task's progress
                        response_future = get_chatgpt_response.submit(user_input, client)
                        
                        # Use the check_task_state function to get the current state and result of the task
                        # This allows us to handle different states (Completed, Failed, Running) appropriately
                        response_state, response_result = check_task_state_manually(response_future)
                    
                    if response_state == "Completed":
                        st.text_area("ChatGPT Response:", value=response_result, height=300)
                    elif response_state == "Failed":
                        st.error(f"Failed to get response: {response_result}")
                    else:
                        st.warning("Response is still processing...")
                else:
                    st.warning("Please enter a message.")
        # elif validation_state == "Completed":
        #     st.error("Invalid API Key. Please check and try again.")
        # elif validation_state == "Failed":
        #     st.error(f"API Key validation failed: {validation_result}")
        else:
            st.warning("API Key validation is still processing...")

if __name__ == "__main__":
    main()


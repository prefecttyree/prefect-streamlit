import streamlit as st
import openai
from prefect import flow, task
from prefect.transactions import transaction


# Set up the page configuration
st.set_page_config(page_title="AI Assistant", layout="wide")

# Sidebar
st.sidebar.header("Settings")
model = st.sidebar.selectbox("Select Model", ["gpt-3.5-turbo", "gpt-4"])
temperature = st.sidebar.slider("Temperature", 0.0, 1.0, 0.7)
api_key = st.sidebar.text_input("Enter OpenAI API Key", type="password")

# Main content
st.title("AI Assistant")

# Input area
user_input = st.text_area("Enter your question or prompt:", height=150)

@task(log_prints=True)
def generate_response(prompt: str, model: str, temperature: float, api_key: str) -> str:
    openai.api_key = api_key
    response = openai.ChatCompletion.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1000,
        temperature=temperature
    )
    return response.choices[0].message.content

@transaction
def validate_api_key(api_key: str):
    if not api_key:
        raise ValueError("Invalid API key. Please enter a valid OpenAI API key.")
    try:
        openai.api_key = api_key
        openai.Model.list()
    except Exception as e:
        raise ValueError(f"API key validation failed: {str(e)}")

@flow(name="Streamlit AI Assistant Flow", log_prints=True)
def ai_assistant_flow(prompt: str, model: str, temperature: float, api_key: str):
    if prompt:
        with st.spinner("Validating API key..."):
            validate_api_key(api_key)
        with st.spinner("Generating response..."):
            response = generate_response(prompt, model, temperature, api_key)
        return response
    return None

# Output area
if st.button("Generate Response"):
    if user_input and api_key:
        try:
            response = ai_assistant_flow(user_input, model, temperature, api_key)
            st.subheader("AI Response:")
            st.write(response)
        except ValueError as e:
            st.error(str(e))
    elif not api_key:
        st.warning("Please enter a valid OpenAI API key in the sidebar.")
    else:
        st.warning("Please enter a question or prompt.")

# Add some information about the app
st.sidebar.markdown("---")
st.sidebar.info(
    "This AI Assistant uses OpenAI's API to generate responses. "
    "Adjust the settings in the sidebar to customize the output."
)

if __name__ == "__main__":
    if api_key:
        ai_assistant_flow(user_input, model, temperature, api_key)
    else:
        st.warning("Please enter a valid OpenAI API key in the sidebar.")
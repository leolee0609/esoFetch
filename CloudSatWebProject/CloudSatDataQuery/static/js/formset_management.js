document.addEventListener('DOMContentLoaded', function () {
    const addButton = document.getElementById('add-form'); // Button to add new form
    const formContainer = document.getElementById('form-container'); // Container for all forms
    const totalForms = document.getElementById('id_form-TOTAL_FORMS'); // Hidden input that tracks total number of forms

    addButton.addEventListener('click', function() {
        let formNum = parseInt(totalForms.value); // Current number of forms
        let newForm = formContainer.children[0].cloneNode(true); // Clone the first form
        let formRegex = RegExp(`form-(\\d+)-`, 'g'); // Regex to find form numbers

        // Update the new form's children elements id and name attributes to reflect new form number
        newForm.innerHTML = newForm.innerHTML.replace(formRegex, `form-${formNum}-`);
        formContainer.appendChild(newForm); // Append the new form to the container
        totalForms.value = formNum + 1; // Increment the form count in the hidden input
    });
});

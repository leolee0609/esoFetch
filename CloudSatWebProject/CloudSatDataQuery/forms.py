from django import forms


class JsonInputForm(forms.Form):
    # Assuming other fields are already defined here
    jobId = forms.CharField(label='Job ID', max_length=100)
    jobType = forms.CharField(label='Job Type', max_length=100)
    dateRangeStart = forms.DateTimeField(label='Start Date')
    dateRangeEnd = forms.DateTimeField(label='End Date')

    # New fields for code submission
    CHOICES = [('sql', 'SQL Query'), ('python', 'Python Script')]
    code_type = forms.ChoiceField(choices=CHOICES, widget=forms.RadioSelect, required=False)
    code = forms.CharField(widget=forms.Textarea(attrs={'cols': 80, 'rows': 10}), required=False, label='Code')

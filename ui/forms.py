# myapp/forms.py

from django import forms
from django.contrib.auth.forms import AuthenticationForm, UserCreationForm
from django.contrib.auth import get_user_model # Best practice to get the active User model

User = get_user_model()

class CustomLoginForm(AuthenticationForm):
    # Override the default fields to add Bootstrap classes
    username = forms.CharField(
        label="Username",
        widget=forms.TextInput(
            attrs={
                # 🌟 This adds the Bootstrap class directly 🌟
                'class': 'form-control',
                'placeholder': 'Enter your username'
            }
        )
    )
    password = forms.CharField(
        label="Password",
        widget=forms.PasswordInput(
            attrs={
                'class': 'form-control'
            }
        )
    )

class CustomUserCreationForm(UserCreationForm):
    class Meta:
        model = User
        fields = ("username", "email") # You can specify which fields to include
        
        # Override the widgets for automatic Bootstrap styling
        widgets = {
            # 🌟 Apply the 'form-control' class to the username field 🌟
            'username': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Enter desired username'}),
            
            # 🌟 Apply the 'form-control' class to the email field 🌟
            'email': forms.EmailInput(attrs={'class': 'form-control', 'placeholder': 'Optional email address'}),
        }

    # Since UserCreationForm uses two password fields, you might want to style them too
    # They are defined outside the Meta class, so you style them in the __init__ method:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Style the two password fields explicitly
        self.fields['password2'].widget.attrs.update({'class': 'form-control'})
        self.fields['password1'].widget.attrs.update({'class': 'form-control'})
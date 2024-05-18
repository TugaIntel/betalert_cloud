import os
from pathlib import Path
from google.cloud import secretmanager

BASE_DIR = Path(__file__).resolve().parent.parent

DEBUG = os.environ.get('GAE_ENV', '').startswith('standard')

# Add your custom domain to ALLOWED_HOSTS
ALLOWED_HOSTS = [
    '*.innate-empire-422116-u4.ew.r.appspot.com',  # Wildcard for App Engine hostnames
    'mybotandai.com',
    'localhost'
]


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'matches',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'betalert.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'betalert.wsgi.application'

# Database
# https://docs.djangoproject.com/en/5.0/ref/settings/#databases


def get_secret_client():
    """Retrieves a Secret Manager client instance."""
    return secretmanager.SecretManagerServiceClient()


def get_secret(secret_name):
    """Retrieves a secret value from Secret Manager."""
    client = get_secret_client()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


PROJECT_ID = 'innate-empire-422116-u4'
SECRET_KEY = get_secret("DJANGO_SECRET_KEY")

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql'
        if os.getenv('GAE_APPLICATION', None) else 'django.db.backends.sqlite3',
        'HOST': '/cloudsql/{}'.format(os.environ['INSTANCE_CONNECTION_NAME'])
        if os.getenv('GAE_APPLICATION', None) else '',
        'NAME': 'BetAlert'
        if os.getenv('GAE_APPLICATION', None) else BASE_DIR / 'db.sqlite3',
        'USER': 'betadmin'
        if os.getenv('GAE_APPLICATION', None) else '',
        'PASSWORD': get_secret("DB_PASS")
        if os.getenv('GAE_APPLICATION', None) else '',
    }
}
# Password validation
# https://docs.djangoproject.com/en/5.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/5.0/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.0/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static')  # Collected static files will go here
STATICFILES_DIRS = []

# Default primary key field type
# https://docs.djangoproject.com/en/5.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

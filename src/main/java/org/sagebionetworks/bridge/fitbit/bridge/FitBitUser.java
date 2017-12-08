package org.sagebionetworks.bridge.fitbit.bridge;

// todo doc
public class FitBitUser {
    private final String accessToken;
    private final String healthCode;
    private final String userId;

    private FitBitUser(String accessToken, String healthCode, String userId) {
        this.accessToken = accessToken;
        this.healthCode = healthCode;
        this.userId = userId;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getHealthCode() {
        return healthCode;
    }

    public String getUserId() {
        return userId;
    }

    public static class Builder {
        private String accessToken;
        private String healthCode;
        private String userId;

        public Builder withAccessToken(String accessToken) {
            this.accessToken = accessToken;
            return this;
        }

        public Builder withHealthCode(String healthCode) {
            this.healthCode = healthCode;
            return this;
        }

        public Builder withUserId(String userId) {
            this.userId = userId;
            return this;
        }

        public FitBitUser build() {
            return new FitBitUser(accessToken, healthCode, userId);
        }
    }
}

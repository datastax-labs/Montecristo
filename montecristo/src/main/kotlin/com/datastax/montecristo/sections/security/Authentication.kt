/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.montecristo.sections.security

import com.datastax.montecristo.logs.Searcher
import com.datastax.montecristo.model.Cluster
import com.datastax.montecristo.model.ConfigSource
import com.datastax.montecristo.model.profiles.ExecutionProfile
import com.datastax.montecristo.sections.DocumentSection
import com.datastax.montecristo.sections.structure.Recommendation
import com.datastax.montecristo.sections.structure.RecommendationPriority
import com.datastax.montecristo.sections.structure.RecommendationType
import com.datastax.montecristo.sections.structure.near

class Authentication : DocumentSection {

    private val customParts = StringBuilder()

    /**
    Check if authentication and authorization are enabled.
    Recommend enabling them if they're not.
     */

    override fun getDocument(
        cluster: Cluster,
        logSearcher: Searcher,
        recs: MutableList<Recommendation>,
        executionProfile: ExecutionProfile
    ): String {
        val args = super.createDocArgs(cluster)

        val authenticator = cluster.getSetting("authenticator", ConfigSource.CASS, "AllowAllAuthenticator")
        val authorizer = cluster.getSetting("authorizer", ConfigSource.CASS, "AllowAllAuthorizer")

        val dseAuthenticatorEnabled = cluster.getSetting("authentication_options.enabled", ConfigSource.DSE, "false")
        val dseAuthorizationEnabled = cluster.getSetting("authorization_options.enabled", ConfigSource.DSE, "false")

        val authenticatorText = authenticator.getSingleValue()
        val authorizationText = authorizer.getSingleValue()

        // Is Authentication consistent, and if its a DSE, is DSE Authentication Consistent?
        if (authenticator.isConsistent() && ((!cluster.isDse) || cluster.isDse && dseAuthenticatorEnabled.isConsistent())) {

            if (cluster.isDse && authenticatorText.endsWith("DseAuthenticator")) {
                // check if the auth is set to use the DSE.yaml values.
                if (dseAuthenticatorEnabled.getSingleValue() == "true") {
                    customParts.append("Authentication is currently enabled using DSE Authenticator : \n\n")
                } else {
                    customParts.append("Authentication is currently using the DSE Authenticator, but DSE authentication is disabled, resulting in no authentication : \n\n")
                }
            } else {
                // using C* settings or something custom, not dse.yaml settings.
                customParts.append("Authentication is currently ${if (authenticatorText.endsWith("AllowAllAuthenticator")) "disabled" else "enabled"} : \n\n")
            }
            customParts.append("```\n")
            customParts.append("authenticator: $authenticatorText \n")
            customParts.append("```\n\n")
        } else {
            customParts.append("Authentication settings are inconsistent across the cluster\n\n")
            customParts.append("```\n")
            authenticator.values.forEach { entry -> customParts.append(entry.key + " = " + entry.value.getConfigValue() + "\n") }
            if (cluster.isDse && !dseAuthenticatorEnabled.isConsistent()) {
                dseAuthenticatorEnabled.values.forEach { entry -> customParts.append(entry.key + " = " + entry.value.getConfigValue() + "\n") }
            }
            customParts.append("```\n\n")
        }

        if (!cluster.isDse && (!authenticator.isConsistent() || authenticator.getSingleValue().endsWith("AllowAllAuthenticator"))) {
            recs.add(Recommendation(RecommendationPriority.NEAR, RecommendationType.SECURITY, authenticatorRecommendation))
        }
        if (cluster.isDse && ((!authenticator.isConsistent() || (authenticatorText.endsWith("DseAuthenticator") && dseAuthenticatorEnabled.getSingleValue() == "false")))) {
            recs.add(Recommendation(RecommendationPriority.NEAR, RecommendationType.SECURITY, dseAuthenticatorRecommendation))
        }


        // Is Authorization consistent, and if its a DSE, is DSE Authorization Consistent?
        if (authorizer.isConsistent() && ((!cluster.isDse) || cluster.isDse && dseAuthorizationEnabled.isConsistent())) {

            if (cluster.isDse && authorizationText.endsWith("DseAuthorizer")) {
                // check if the auth is set to use the DSE.yaml values.
                if (dseAuthorizationEnabled.getSingleValue() == "true") {
                    customParts.append("Authorization is currently enabled using DSE Authorizer : \n\n")
                } else {
                    customParts.append("Authorization is currently using the DSE Authorizer, but DSE authorization is disabled, resulting in no authorization : \n\n")
                }
            } else {
                // using C* settings or something custom, not dse.yaml settings.
                customParts.append("Authorization is currently ${if (authorizationText.endsWith("AllowAllAuthorizer")) "disabled" else "enabled"} : \n\n")
            }
            customParts.append("```\n")
            customParts.append("authorizer: $authorizationText \n")
            customParts.append("```\n\n")
        } else {
            customParts.append("Authorization settings are inconsistent across the cluster\n\n")
            customParts.append("```\n")
            authorizer.values.forEach { entry -> customParts.append(entry.key + " = " + entry.value.getConfigValue() + "\n") }
            if (cluster.isDse && !dseAuthorizationEnabled.isConsistent()) {
                dseAuthorizationEnabled.values.forEach { entry -> customParts.append(entry.key + " = " + entry.value.getConfigValue() + "\n") }
            }
            customParts.append("```\n\n")
        }

        if (!cluster.isDse && (!authorizer.isConsistent() || authorizationText.endsWith("AllowAllAuthorizer"))) {
            recs.add(Recommendation(RecommendationPriority.NEAR, RecommendationType.SECURITY, authorizerRecommendation))
        }
        if (cluster.isDse && ((!authorizer.isConsistent() || (authorizationText.endsWith("DseAuthorizer") && dseAuthorizationEnabled.getSingleValue() == "false")))) {
            recs.add(Recommendation(RecommendationPriority.NEAR, RecommendationType.SECURITY, dseAuthorizerRecommendation))
        }

        // for displaying the noted for reference - if we have hit any recommendation at this stage
        // then authentication or authorization is not properly enabled.
        val noAuth = recs.size > 0
        args["noAuth"] = noAuth
        checkAuthSettings(cluster, recs)
        args["authSettings"] = customParts.toString()

        return compileAndExecute("security/security_authentication.md", args)
    }

    internal fun checkAuthSettings(cluster: Cluster, recs: MutableList<Recommendation>) {
        // check auth validity times - there is significant complexity on which values, appear in which versions of C* / DSE and with what default as follows:
        // permissions_validity_in_ms / permissions_update_interval_in_ms - appears in all
        // credentials_validity_in_ms / credentials_update_interval_in_ms - DSE 5.1+ (but removed in DSE 6.x) and C* 3.4+ only
        // roles_validity_in_ms / roles_update_interval_in_ms  - appears in all
        //
        // defaults : C* 2.x, 3.x, DSE 4.x and 5.x is 2000
        // all defaults in DSE 6.x are 120000
        val default: String = cluster.databaseVersion.defaultPermissionsValidity()

        val permissionsValidityInMs = cluster.getSetting("permissions_validity_in_ms", ConfigSource.CASS, default)
        val permissionsUpdateIntervalInMs = cluster.getSetting("permissions_update_interval_in_ms", ConfigSource.CASS, default)
        val rolesValidityInMs = cluster.getSetting("roles_validity_in_ms", ConfigSource.CASS, default)
        val rolesUpdateIntervalInMs = cluster.getSetting("roles_update_interval_in_ms", ConfigSource.CASS, default)

        // check values are consistent
        val inconsistentSettingsBuilder = StringBuilder()
        if (!permissionsValidityInMs.isConsistent()) {
            inconsistentSettingsBuilder.append("permissions_validity_in_ms, ")
        }
        if (!permissionsUpdateIntervalInMs.isConsistent()) {
            inconsistentSettingsBuilder.append("permissions_update_interval_in_ms, ")
        }
        if (!rolesValidityInMs.isConsistent()) {
            inconsistentSettingsBuilder.append("roles_validity_in_ms, ")
        }
        if (!rolesUpdateIntervalInMs.isConsistent()) {
            inconsistentSettingsBuilder.append("roles_update_interval_in_ms, ")
        }

        // check values are at least 30 seconds.
        val settingsTooLowBuilder = StringBuilder()
        if (permissionsValidityInMs.getDistinctValues().minOf { it.value.toIntOrNull() ?: 0 } < 30000) {
            settingsTooLowBuilder.append("permissions_validity_in_ms, ")
        }
        if (permissionsUpdateIntervalInMs.getDistinctValues().minOf { it.value.toIntOrNull() ?: 0 } < 30000) {
            settingsTooLowBuilder.append("permissions_update_interval_in_ms, ")
        }
        if (rolesValidityInMs.getDistinctValues().minOf { it.value.toIntOrNull() ?: 0 } < 30000) {
            settingsTooLowBuilder.append("roles_validity_in_ms, ")
        }
        if (rolesUpdateIntervalInMs.getDistinctValues().minOf { it.value.toIntOrNull() ?: 0 } < 30000) {
            settingsTooLowBuilder.append("roles_update_interval_in_ms, ")
        }

        if (cluster.databaseVersion.supportsCredentialValiditySetting()) {
            val credentialsValidityInMs = cluster.getSetting("credentials_validity_in_ms", ConfigSource.CASS, "2000")
            val credentialsUpdateIntervalInMs = cluster.getSetting("credentials_update_interval_in_ms", ConfigSource.CASS,"2000")

            // check if we add to inconsistent list
            if (!credentialsValidityInMs.isConsistent()) {
                inconsistentSettingsBuilder.append("credentials_validity_in_ms, ")
            }
            if (!credentialsUpdateIntervalInMs.isConsistent()) {
                inconsistentSettingsBuilder.append("credentials_update_interval_in_ms, ")
            }

            // check if we add to the too low value list
            if (credentialsValidityInMs.getDistinctValues().minOf { it.value.toIntOrNull() ?: 0 } < 30000) {
                settingsTooLowBuilder.append("credentials_validity_in_ms, ")
            }
            if (credentialsUpdateIntervalInMs.getDistinctValues().minOf { it.value.toIntOrNull() ?: 0 } < 30000) {
                settingsTooLowBuilder.append("credentials_update_interval_in_ms, ")
            }
        }

        val inconsistentSettings = inconsistentSettingsBuilder.toString()
        if (inconsistentSettings.isNotEmpty()) {
            // we have inconsistent auth settings, issue a recommendation
            recs.near(RecommendationType.SECURITY,authInconsistentSettingsRecommendation + inconsistentSettings.dropLast(2)) // remove final comma-space.
        }

        // check values are at least 30 seconds.
        val settingsTooLow = settingsTooLowBuilder.toString()
        if (settingsTooLow.isNotEmpty()) {
            // we have some auth settings which are too low, issue the recommendation
            recs.near(RecommendationType.SECURITY,authValidityRecommendation + settingsTooLow.dropLast(2)) // remove final comma-space.
        }
    }

    internal val authenticatorRecommendation = "We recommend enabling authentication by setting `authenticator` to `PasswordAuthenticator` in cassandra.yaml to improve security."
    internal val dseAuthenticatorRecommendation = "We recommend enabling authentication by setting authenticator to `com.datastax.bdp.cassandra.auth.DseAuthenticator` in cassandra.yaml, and `enabled` to `true` in `authentication_options` section of dse.yaml to improve security."

    internal val authorizerRecommendation = "We recommend enabling authorization by setting `authorizer` to `CassandraAuthorizer` in cassandra.yaml to improve security."
    internal val dseAuthorizerRecommendation = "We recommend enabling authorization by setting authorizer to `com.datastax.bdp.cassandra.auth.DseAuthorizer` in cassandra.yaml, and `enabled` to `true` in `authorization_options` section of dse.yaml to improve security."

    private val authInconsistentSettingsRecommendation = "We recommend that the authentication settings on the nodes are made consistent, the following settings are not consistent : "
    private val authValidityRecommendation = "We recommend to increase the following settings to a value of at least 30000 (ie 30 seconds) : "

}

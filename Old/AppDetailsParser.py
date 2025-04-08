import json

class AppDetailsParser:
    def __init__(self, file_path):
        # Load the data when initializing the class
        with open(file_path, 'r') as file:
            self.data = json.load(file)
        
    
    def get_success(self, app_id):
        return self.data.get(app_id, {}).get("success", False)

    def get_data(self, app_id):
        return self.data.get(app_id, {}).get('data', {})
    
    def get_type(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('type', '')

    def get_name(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('name', '')
        
    def get_steamappId(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('steam_appid', 0)
        
    def get_required_age(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('required_age', '')
        
    def get_is_free(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('is_free', False)

    def get_controller_support(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('controller_support', '')
        
    def get_dlc(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('dlc', [])
        
    def get_detailed_description(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('detailed_description', '')

    def get_about_the_game(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('about_the_game', '')
        
    def get_short_description(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('short_description', '')
        
    def get_supported_languages(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('supported_languages', '')

    def get_header_image(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('header_image', '')
        
    def get_capsule_image(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('capsule_image', '')

    def get_capsule_imagev5(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('capsule_imagev5', '')
        
    def get_website(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('website', '')
    
    def get_pc_requirements(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('pc_requirements', {})
        
    def get_pc_requirements_minimum(self, app_id):
        if self.get_success(app_id):
            pc_req = self.get_pc_requirements(app_id)
            return pc_req.get('minimum', '')

    def get_pc_requirements_recommended(self, app_id):
        if self.get_success(app_id):
            pc_req = self.get_pc_requirements(app_id)
            return pc_req.get('recommended', '')

    def get_mac_requirements(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('mac_requirements', {})
        
    def get_mac_requirements_minimum(self, app_id):
        if self.get_success(app_id):
            mac_req = self.get_mac_requirements(app_id)
            return mac_req.get('minimum', '')
            
    def get_mac_requirements_recommended(self, app_id):
        if self.get_success(app_id):
            mac_req = self.get_mac_requirements(app_id)
            return mac_req.get('recommended', '')
    
    def get_linux_requirements(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('linux_requirements', {})

    def get_linux_requirements_minimum(self, app_id):
        if self.get_success(app_id):
            linux_req = self.get_linux_requirements(app_id)
            return linux_req.get('minimum', '')
        
    def get_linux_requirements_recommended(self, app_id):
        if self.get_success(app_id):
            linux_req = self.get_linux_requirements(app_id)
            return linux_req.get('recommended', '')
        
    def get_legal_notice(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('legal_notice', '')
            
    def get_developers(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('developers', [])
    
    def get_publishers(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('publishers', [])
    
    def get_price_overview(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('price_overview', {})
    
    def get_currency(self, app_id):
        if self.get_success(app_id):
            price_overview = self.get_price_overview(app_id)
            return price_overview.get('currency', '')
    
    def get_initial_price(self, app_id):
        if self.get_success(app_id):
            price_overview = self.get_price_overview(app_id)
            return price_overview.get('initial', 0)
    
    def get_final_price(self, app_id):
        if self.get_success(app_id):
            price_overview = self.get_price_overview(app_id)
            return price_overview.get('final', 0)
    
    def get_discount_percent(self, app_id):
        if self.get_success(app_id):
            price_overview = self.get_price_overview(app_id)
            return price_overview.get('discount_percent', 0)
    
    def get_initial_formatted(self, app_id):
        if self.get_success(app_id):
            price_overview = self.get_price_overview(app_id)
            return price_overview.get('initial_formatted', '')
    
    def get_final_formatted(self, app_id):
        if self.get_success(app_id):
            price_overview = self.get_price_overview(app_id)
            return price_overview.get('final_formatted', '')
    
    def get_packages(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('packages', [])
    
    def get_package_groups(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('package_groups', [])
    
    def get_package_group(self, app_id, index=0):
        """Get a specific package group by index"""
        if self.get_success(app_id):
            package_groups = self.get_package_groups(app_id)
            if package_groups and len(package_groups) > index:
                return package_groups[index]
            return {}

    def get_package_group_name(self, app_id, index=0):
        """Get the name of a specific package group"""
        package_group = self.get_package_group(app_id, index)
        return package_group.get('name', '')
    
    def get_package_group_title(self, app_id, index=0):
        """Get the title of a specific package group"""
        package_group = self.get_package_group(app_id, index)
        return package_group.get('title', '')
    
    def get_package_group_description(self, app_id, index=0):
        """Get the description of a specific package group"""
        package_group = self.get_package_group(app_id, index)
        return package_group.get('description', '')
    
    def get_package_group_selection_text(self, app_id, index=0):
        """Get the selection_text of a specific package group"""
        package_group = self.get_package_group(app_id, index)
        return package_group.get('selection_text', '')
    
    def get_package_group_save_text(self, app_id, index=0):
        """Get the save_text of a specific package group"""
        package_group = self.get_package_group(app_id, index)
        return package_group.get('save_text', '')
    
    def get_package_group_display_type(self, app_id, index=0):
        """Get the display_type of a specific package group"""
        package_group = self.get_package_group(app_id, index)
        return package_group.get('display_type', 0)
    
    def get_package_group_is_recurring_subscription(self, app_id, index=0):
        """Get is_recurring_subscription of a specific package group"""
        package_group = self.get_package_group(app_id, index)
        return package_group.get('is_recurring_subscription', '')
    
    def get_package_group_subs(self, app_id, index=0):
        """Get the subs list of a specific package group"""
        package_group = self.get_package_group(app_id, index)
        return package_group.get('subs', [])
    
    def get_package_group_sub(self, app_id, group_index=0, sub_index=0):
        """Get a specific sub from a package group by index"""
        subs = self.get_package_group_subs(app_id, group_index)
        if subs and len(subs) > sub_index:
            return subs[sub_index]
        return {}
    
    def get_package_group_sub_packageid(self, app_id, group_index=0, sub_index=0):
        """Get packageid of a specific sub"""
        sub = self.get_package_group_sub(app_id, group_index, sub_index)
        return sub.get('packageid', 0)
    
    def get_package_group_sub_percent_savings_text(self, app_id, group_index=0, sub_index=0):
        """Get percent_savings_text of a specific sub"""
        sub = self.get_package_group_sub(app_id, group_index, sub_index)
        return sub.get('percent_savings_text', '')
    
    def get_package_group_sub_percent_savings(self, app_id, group_index=0, sub_index=0):
        """Get percent_savings of a specific sub"""
        sub = self.get_package_group_sub(app_id, group_index, sub_index)
        return sub.get('percent_savings', 0)
    
    def get_package_group_sub_option_text(self, app_id, group_index=0, sub_index=0):
        """Get option_text of a specific sub"""
        sub = self.get_package_group_sub(app_id, group_index, sub_index)
        return sub.get('option_text', '')
    
    def get_package_group_sub_option_description(self, app_id, group_index=0, sub_index=0):
        """Get option_description of a specific sub"""
        sub = self.get_package_group_sub(app_id, group_index, sub_index)
        return sub.get('option_description', '')
    
    def get_package_group_sub_can_get_free_license(self, app_id, group_index=0, sub_index=0):
        """Get can_get_free_license of a specific sub"""
        sub = self.get_package_group_sub(app_id, group_index, sub_index)
        return sub.get('can_get_free_license', '')
    
    def get_package_group_sub_is_free_license(self, app_id, group_index=0, sub_index=0):
        """Get is_free_license of a specific sub"""
        sub = self.get_package_group_sub(app_id, group_index, sub_index)
        return sub.get('is_free_license', False)
    
    def get_package_group_sub_price_in_cents_with_discount(self, app_id, group_index=0, sub_index=0):
        """Get price_in_cents_with_discount of a specific sub"""
        sub = self.get_package_group_sub(app_id, group_index, sub_index)
        return sub.get('price_in_cents_with_discount', 0)
    
    def get_platforms(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('platforms', {})
    
    def is_windows_supported(self, app_id):
        if self.get_success(app_id):
            platforms = self.get_platforms(app_id)
            return platforms.get('windows', False)
    
    def is_mac_supported(self, app_id):
        if self.get_success(app_id):
            platforms = self.get_platforms(app_id)
            return platforms.get('mac', False)
    
    def is_linux_supported(self, app_id):
        if self.get_success(app_id):
            platforms = self.get_platforms(app_id)
            return platforms.get('linux', False)
    
    def get_metacritic(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('metacritic', {})
    
    def get_metacritic_score(self, app_id):
        if self.get_success(app_id):
            metacritic = self.get_metacritic(app_id)
            return metacritic.get('score', 0)
    
    def get_metacritic_url(self, app_id):
        if self.get_success(app_id):
            metacritic = self.get_metacritic(app_id)
            return metacritic.get('url', '')
    
    def get_categories(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('categories', [])
    
    def get_category(self, app_id, index=0):
        """Get a specific category by index"""
        categories = self.get_categories(app_id)
        if categories and len(categories) > index:
            return categories[index]
        return {}
    
    def get_category_id(self, app_id, index=0):
        """Get id of a specific category"""
        category = self.get_category(app_id, index)
        return category.get('id', 0)
    
    def get_category_description(self, app_id, index=0):
        """Get description of a specific category"""
        category = self.get_category(app_id, index)
        return category.get('description', '')
    
    def get_genres(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('genres', [])
    
    def get_genre(self, app_id, index=0):
        """Get a specific genre by index"""
        genres = self.get_genres(app_id)
        if genres and len(genres) > index:
            return genres[index]
        return {}
    
    def get_genre_id(self, app_id, index=0):
        """Get id of a specific genre"""
        genre = self.get_genre(app_id, index)
        return genre.get('id', '')
    
    def get_genre_description(self, app_id, index=0):
        """Get description of a specific genre"""
        genre = self.get_genre(app_id, index)
        return genre.get('description', '')
    
    def get_screenshots(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('screenshots', [])
    
    def get_screenshot(self, app_id, index=0):
        """Get a specific screenshot by index"""
        screenshots = self.get_screenshots(app_id)
        if screenshots and len(screenshots) > index:
            return screenshots[index]
        return {}
    
    def get_screenshot_id(self, app_id, index=0):
        """Get id of a specific screenshot"""
        screenshot = self.get_screenshot(app_id, index)
        return screenshot.get('id', 0)
    
    def get_screenshot_path_thumbnail(self, app_id, index=0):
        """Get path_thumbnail of a specific screenshot"""
        screenshot = self.get_screenshot(app_id, index)
        return screenshot.get('path_thumbnail', '')
    
    def get_screenshot_path_full(self, app_id, index=0):
        """Get path_full of a specific screenshot"""
        screenshot = self.get_screenshot(app_id, index)
        return screenshot.get('path_full', '')
    
    def get_movies(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('movies', [])
    
    def get_movie(self, app_id, index=0):
        """Get a specific movie by index"""
        movies = self.get_movies(app_id)
        if movies and len(movies) > index:
            return movies[index]
        return {}
    
    def get_movie_id(self, app_id, index=0):
        """Get id of a specific movie"""
        movie = self.get_movie(app_id, index)
        return movie.get('id', 0)
    
    def get_movie_name(self, app_id, index=0):
        """Get name of a specific movie"""
        movie = self.get_movie(app_id, index)
        return movie.get('name', '')
    
    def get_movie_thumbnail(self, app_id, index=0):
        """Get thumbnail of a specific movie"""
        movie = self.get_movie(app_id, index)
        return movie.get('thumbnail', '')
    
    def get_movie_webm(self, app_id, index=0):
        """Get webm object of a specific movie"""
        movie = self.get_movie(app_id, index)
        return movie.get('webm', {})
    
    def get_movie_webm_480(self, app_id, index=0):
        """Get 480 resolution webm url of a specific movie"""
        webm = self.get_movie_webm(app_id, index)
        return webm.get('480', '')
    
    def get_movie_webm_max(self, app_id, index=0):
        """Get max resolution webm url of a specific movie"""
        webm = self.get_movie_webm(app_id, index)
        return webm.get('max', '')
    
    def get_movie_mp4(self, app_id, index=0):
        """Get mp4 object of a specific movie"""
        movie = self.get_movie(app_id, index)
        return movie.get('mp4', {})
    
    def get_movie_mp4_480(self, app_id, index=0):
        """Get 480 resolution mp4 url of a specific movie"""
        mp4 = self.get_movie_mp4(app_id, index)
        return mp4.get('480', '')
    
    def get_movie_mp4_max(self, app_id, index=0):
        """Get max resolution mp4 url of a specific movie"""
        mp4 = self.get_movie_mp4(app_id, index)
        return mp4.get('max', '')
    
    def get_movie_highlight(self, app_id, index=0):
        """Get highlight status of a specific movie"""
        movie = self.get_movie(app_id, index)
        return movie.get('highlight', False)
    
    def get_recommendations(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('recommendations', {})
    
    def get_total_recommendations(self, app_id):
        if self.get_success(app_id):
            recommendations = self.get_recommendations(app_id)
            return recommendations.get('total', 0)
    
    def get_achievements(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('achievements', {})
    
    def get_total_achievements(self, app_id):
        if self.get_success(app_id):
            achievements = self.get_achievements(app_id)
            return achievements.get('total', 0)
    
    def get_highlighted_achievements(self, app_id):
        if self.get_success(app_id):
            achievements = self.get_achievements(app_id)
            return achievements.get('highlighted', [])
    
    def get_highlighted_achievement(self, app_id, index=0):
        """Get a specific highlighted achievement by index"""
        highlighted = self.get_highlighted_achievements(app_id)
        if highlighted and len(highlighted) > index:
            return highlighted[index]
        return {}
    
    def get_highlighted_achievement_name(self, app_id, index=0):
        """Get name of a specific highlighted achievement"""
        achievement = self.get_highlighted_achievement(app_id, index)
        return achievement.get('name', '')
    
    def get_highlighted_achievement_path(self, app_id, index=0):
        """Get path of a specific highlighted achievement"""
        achievement = self.get_highlighted_achievement(app_id, index)
        return achievement.get('path', '')
    
    def get_release_date(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('release_date', {})
    
    def is_coming_soon(self, app_id):
        if self.get_success(app_id):
            release_date = self.get_release_date(app_id)
            return release_date.get('coming_soon', False)
    
    def get_release_date_string(self, app_id):
        if self.get_success(app_id):
            release_date = self.get_release_date(app_id)
            return release_date.get('date', '')
    
    def get_support_info(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('support_info', {})
    
    def get_support_url(self, app_id):
        if self.get_success(app_id):
            support_info = self.get_support_info(app_id)
            return support_info.get('url', '')
    
    def get_support_email(self, app_id):
        if self.get_success(app_id):
            support_info = self.get_support_info(app_id)
            return support_info.get('email', '')
    
    def get_background(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('background', '')
    
    def get_background_raw(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('background_raw', '')
    
    def get_content_descriptors(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('content_descriptors', {})
    
    def get_content_descriptor_ids(self, app_id):
        if self.get_success(app_id):
            content_descriptors = self.get_content_descriptors(app_id)
            return content_descriptors.get('ids', [])
    
    def get_content_descriptor_notes(self, app_id):
        if self.get_success(app_id):
            content_descriptors = self.get_content_descriptors(app_id)
            return content_descriptors.get('notes', '')
    
    def get_ratings(self, app_id):
        if self.get_success(app_id):
            return self.get_data(app_id).get('ratings', {})
    
    def get_rating_by_system(self, app_id, system_name):
        """
        Get rating information for a specific rating system.
        
        Parameters:
        - app_id: The Steam app ID
        - system_name: The rating system name, one of:
          * 'esrb' - Entertainment Software Rating Board (North America)
          * 'pegi' - Pan European Game Information (Europe)
          * 'oflc' - Office of Film and Literature Classification (Australia)
          * 'nzoflc' - Office of Film and Literature Classification (New Zealand)
          * 'kgrb' - Korea Game Rating Board
          * 'dejus' - Department of Justice, Rating, Titles and Qualification (Brazil)
          * 'mda' - Media Development Authority (Singapore)
          * 'csrr' - Computer Software Rating Regulation (Taiwan)
          * 'crl' - Content Rating Law (Russia)
          * 'usk' - Unterhaltungssoftware Selbstkontrolle (Germany)
          * 'steam_germany' - Steam-specific rating for Germany
        
        Returns:
        - Dictionary containing rating information for the specified system
        """
        if self.get_success(app_id):
            ratings = self.get_ratings(app_id)
            return ratings.get(system_name, {})
    
    def get_rating_value(self, app_id, system_name):
        """
        Get rating value for any rating system.
        
        Examples:
        - ESRB rating: get_rating_value(app_id, 'esrb')
        - PEGI rating: get_rating_value(app_id, 'pegi')
        - OFLC rating: get_rating_value(app_id, 'oflc')
        - USK rating: get_rating_value(app_id, 'usk')
        - Any other: get_rating_value(app_id, 'kgrb'), get_rating_value(app_id, 'dejus'), etc.
        """
        rating = self.get_rating_by_system(app_id, system_name)
        return rating.get('rating', '')
    
    def get_rating_descriptors(self, app_id, system_name):
        """
        Get descriptors for any rating system.
        
        Examples:
        - ESRB descriptors: get_rating_descriptors(app_id, 'esrb')
        - PEGI descriptors: get_rating_descriptors(app_id, 'pegi')
        - German descriptors: get_rating_descriptors(app_id, 'steam_germany')
        """
        rating = self.get_rating_by_system(app_id, system_name)
        return rating.get('descriptors', '')
    
    def get_rating_required_age(self, app_id, system_name):
        """
        Get required age for any rating system.
        
        Examples:
        - ESRB required age: get_rating_required_age(app_id, 'esrb')
        - PEGI required age: get_rating_required_age(app_id, 'pegi')
        """
        rating = self.get_rating_by_system(app_id, system_name)
        return rating.get('required_age', '')
    
    def get_rating_use_age_gate(self, app_id, system_name):
        """
        Get use_age_gate flag for any rating system.
        
        Examples:
        - ESRB age gate: get_rating_use_age_gate(app_id, 'esrb')
        - PEGI age gate: get_rating_use_age_gate(app_id, 'pegi')
        """
        rating = self.get_rating_by_system(app_id, system_name)
        return rating.get('use_age_gate', '')
    
    def get_rating_banned(self, app_id, system_name):
        """
        Get banned status for any rating system that uses it.
        
        Example:
        - German rating banned status: get_rating_banned(app_id, 'steam_germany')
        """
        rating = self.get_rating_by_system(app_id, system_name)
        return rating.get('banned', '')
    
    def get_rating_generated(self, app_id, system_name):
        """
        Get rating_generated flag for any rating system that uses it.
        
        Example:
        - German rating generated flag: get_rating_generated(app_id, 'steam_germany')
        """
        rating = self.get_rating_by_system(app_id, system_name)
        return rating.get('rating_generated', '')
    
    # Generic methods for dynamic property access
    def get_property(self, app_id, property_path):
        """
        Generically access a property by path.
        Property path is a dot-separated string like 'data.price_overview.currency'
        
        Examples:
        - ESRB rating: get_property(app_id, 'data.ratings.esrb.rating')
        - PEGI descriptors: get_property(app_id, 'data.ratings.pegi.descriptors')
        - OFLC required age: get_property(app_id, 'data.ratings.oflc.required_age')
        - USK use_age_gate: get_property(app_id, 'data.ratings.usk.use_age_gate')
        - First category ID: get_property(app_id, 'data.categories[0].id')
        """
        if not self.get_success(app_id):
            return None
        
        parts = property_path.split('.')
        current = self.data.get(app_id, {})
        
        for part in parts:
            # Handle array indexing with [index] syntax
            if '[' in part and part.endswith(']'):
                name, index_str = part.split('[')
                index = int(index_str.rstrip(']'))
                current = current.get(name, [])
                if isinstance(current, list) and len(current) > index:
                    current = current[index]
                else:
                    return None
            else:
                if isinstance(current, dict):
                    current = current.get(part, None)
                else:
                    return None
                
            if current is None:
                return None
                
        return current

# Example usage:
if __name__ == "__main__":
    parser = AppDetailsParser('App_Details/App_Details1091500.json')
    app_id = '1091500'
    print(f"App Name: {parser.get_name(app_id)}")
    print(f"Is Free: {parser.get_is_free(app_id)}")
    print(f"Platforms: Windows={parser.is_windows_supported(app_id)}, Mac={parser.is_mac_supported(app_id)}, Linux={parser.is_linux_supported(app_id)}")
    print(f"Metacritic Score: {parser.get_metacritic_score(app_id)}")
    print(f"Total Recommendations: {parser.get_total_recommendations(app_id)}")
    
    # Examples of accessing different rating systems
    print(f"ESRB Rating: {parser.get_rating_value(app_id, 'esrb')}")
    print(f"PEGI Rating: {parser.get_rating_value(app_id, 'pegi')}")
    print(f"OFLC Rating: {parser.get_rating_value(app_id, 'oflc')}")
    print(f"USK Rating: {parser.get_rating_value(app_id, 'usk')}")
    
    # Example of accessing a specific rating system property
    print(f"ESRB Descriptors: {parser.get_rating_descriptors(app_id, 'esrb')}")
    print(f"German banned status: {parser.get_rating_banned(app_id, 'steam_germany')}")
    
    # Dynamic property access examples
    print(f"KGRB required age: {parser.get_property(app_id, 'data.ratings.kgrb.required_age')}")
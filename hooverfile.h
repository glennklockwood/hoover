struct hoover_tube_config {
    char dir[PATH_MAX];
};

struct hoover_tube {
    char dir[PATH_MAX];
};

struct hoover_tube *create_hoover_tube(struct hoover_tube_config *config);
void free_hoover_tube(struct hoover_tube *tube);

struct hoover_tube_config *read_tube_config(void);
void save_tube_config(struct hoover_tube_config *config, FILE *out);
void free_tube_config(struct hoover_tube_config *config);

void hoover_send_message(struct hoover_tube *tube,
                         struct hoover_data_obj *hdo,
                         struct hoover_header *header);
